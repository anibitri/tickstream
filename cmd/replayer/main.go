// Command replayer reads archived trades for a time range and publishes them
// to Kafka in event-time order, at real speed, a multiple of it, or as fast as
// possible. By default it writes to an isolated replay.<run_id>.trades topic so
// live data is never mixed with a replay.
//
//	replayer -from 2026-10-04T13:00:00Z -to 2026-10-04T14:00:00Z -speed max -run-id r1
//
// With -generate it writes a synthetic dataset to the archive instead (used
// for test fixtures and benchmarks):
//
//	replayer -generate -dir testdata/archive -minutes 20 -tps 10
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
	"github.com/anibitri/tickstream/internal/replay"
	"github.com/anibitri/tickstream/internal/storage"
)

type Config struct {
	config.Common
	config.Kafka
	config.AWS
	Bucket string `env:"ARCHIVE_BUCKET" envDefault:"tickstream-archive"`
}

func (c *Config) Validate() error { return config.ValidateAll(c.Common, c.Kafka) }

type flags struct {
	from, to        time.Time
	symbols         []string
	speed           float64
	runID, topic    string
	dir             string
	generate        bool
	minutes, seed   int
	tradesPerSecond float64
}

func parseFlags(args []string) (flags, error) {
	fs := flag.NewFlagSet("replayer", flag.ContinueOnError)
	from := fs.String("from", "", "start of the range (RFC 3339, inclusive)")
	to := fs.String("to", "", "end of the range (RFC 3339, exclusive)")
	symbols := fs.String("symbols", "", "comma-separated symbols (default: all)")
	speed := fs.String("speed", "max", "1, 10, any multiple of real time, or max")
	runID := fs.String("run-id", "", "replay run id; output goes to replay.<run-id>.trades")
	topic := fs.String("topic", "", "publish to this topic instead (e.g. md.trades)")
	dir := fs.String("dir", "", "read/write the archive in this local folder instead of S3")
	gen := fs.Bool("generate", false, "write a synthetic dataset instead of replaying")
	minutes := fs.Int("minutes", 20, "-generate: length of the dataset")
	start := fs.String("start", "2026-10-04T13:50:00Z", "-generate: first trade time")
	tps := fs.Float64("tps", 10, "-generate: trades per second across all symbols")
	seed := fs.Int("seed", 1, "-generate: random seed")
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	f := flags{runID: *runID, topic: *topic, dir: *dir, generate: *gen, minutes: *minutes, seed: *seed, tradesPerSecond: *tps}
	if *symbols != "" {
		f.symbols = strings.Split(*symbols, ",")
	}
	if f.generate {
		t, err := time.Parse(time.RFC3339, *start)
		f.from = t
		return f, err
	}
	var err error
	if f.from, err = time.Parse(time.RFC3339, *from); err != nil {
		return f, fmt.Errorf("-from: %w", err)
	}
	if f.to, err = time.Parse(time.RFC3339, *to); err != nil {
		return f, fmt.Errorf("-to: %w", err)
	}
	if !f.to.After(f.from) {
		return f, errors.New("-to must be after -from")
	}
	if *speed != "max" {
		if f.speed, err = strconv.ParseFloat(*speed, 64); err != nil || f.speed <= 0 {
			return f, fmt.Errorf("-speed must be a positive number or max")
		}
	}
	if f.topic == "" {
		if f.runID == "" {
			return f, errors.New("set -run-id (or -topic)")
		}
		f.topic = "replay." + f.runID + ".trades"
	}
	return f, nil
}

func archiveStore(ctx context.Context, cfg Config, dir string) (storage.ObjectStore, error) {
	if dir != "" {
		return &storage.DirStore{Root: dir}, nil
	}
	ac, err := storage.LoadAWS(ctx, cfg.AWS)
	if err != nil {
		return nil, err
	}
	return &storage.S3Store{Client: storage.NewS3(ac, cfg.Endpoint), Bucket: cfg.Bucket}, nil
}

// ensureTopic creates an isolated replay topic (6 partitions, kept for 1 day).
func ensureTopic(ctx context.Context, cl *kgo.Client, topic string) error {
	retention := strconv.Itoa(int((24 * time.Hour).Milliseconds()))
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 6, -1, map[string]*string{"retention.ms": &retention}, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return resp.Err
	}
	return nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("replayer", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		store, err := archiveStore(ctx, cfg, f.dir)
		if err != nil {
			return err
		}
		if f.generate {
			gc := replay.DefaultGenConfig()
			gc.Seed, gc.Start = uint64(f.seed), f.from
			gc.Duration, gc.TradesPerSec = time.Duration(f.minutes)*time.Minute, f.tradesPerSecond
			n, err := replay.Generate(ctx, store, gc)
			if err == nil {
				o.Log.Info("generated dataset", "trades", n, "start", gc.Start, "duration", gc.Duration.String())
			}
			return err
		}

		cl, err := kafkax.NewClient(cfg.Kafka)
		if err != nil {
			return err
		}
		defer cl.Close()
		if strings.HasPrefix(f.topic, "replay.") {
			if err := ensureTopic(ctx, cl, f.topic); err != nil {
				return fmt.Errorf("create %s: %w", f.topic, err)
			}
		}
		r, err := replay.Open(ctx, store, f.symbols, f.from, f.to)
		if err != nil {
			return err
		}
		o.SetReady(true)
		return run(ctx, o, cl, r, f)
	})
}

func run(ctx context.Context, o *observability.Obs, cl *kgo.Client, r *replay.Reader, f flags) error {
	var failed atomic.Int64
	pacer := &replay.Pacer{Speed: f.speed}
	start, lastLog := time.Now(), time.Now()
	n := 0
	for {
		t, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := pacer.Wait(ctx, t.EventTimeNs); err != nil {
			return err
		}
		rec, err := kafkax.ProtoRecord(f.topic, t.Symbol, t)
		if err != nil {
			return err
		}
		kafkax.SetHeader(rec, kafkax.HeaderRecvTimeNs, strconv.FormatInt(t.RecvTimeNs, 10))
		// Produce blocks once the client buffer is full, which gives natural
		// backpressure when running at max speed.
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
			}
		})
		n++
		if time.Since(lastLog) > 5*time.Second {
			lastLog = time.Now()
			o.Log.Info("replaying", "trades", n, "per_sec", int(float64(n)/time.Since(start).Seconds()),
				"at", time.Unix(0, t.EventTimeNs).UTC().Format(time.RFC3339))
		}
	}
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	el := time.Since(start)
	o.Log.Info("replay finished", "topic", f.topic, "trades", n, "seconds", el.Seconds(),
		"per_sec", int(float64(n)/el.Seconds()), "duplicates_skipped", r.Dupes, "failed", failed.Load())
	if failed.Load() > 0 {
		return fmt.Errorf("%d records failed to publish", failed.Load())
	}
	return nil
}
