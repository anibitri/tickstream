// Package storage is the persistence layer: the S3 trade archive (Parquet
// files) and the DynamoDB tables.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/observability"
)

// LoadAWS builds the AWS SDK config. If cfg.Endpoint is set (LocalStack), all
// clients talk to it. Credentials come from the usual AWS_* variables.
func LoadAWS(ctx context.Context, cfg config.AWS) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.Endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(cfg.Endpoint))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// NewS3 returns an S3 client. LocalStack needs path-style URLs.
func NewS3(ac aws.Config, endpoint string) *s3.Client {
	return s3.NewFromConfig(ac, func(o *s3.Options) { o.UsePathStyle = endpoint != "" })
}

// ObjectStore is the part of S3 we use. DirStore does the same on a local
// folder, so tests and fixtures run the same code without S3.
type ObjectStore interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
}

// ErrNotFound is returned by Get for missing keys.
var ErrNotFound = errors.New("object not found")

// S3Store stores objects in one bucket.
type S3Store struct {
	Client *s3.Client
	Bucket string
}

func (s *S3Store) Put(ctx context.Context, key string, body []byte, contentType string) error {
	start := time.Now()
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	observability.S3WriteDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.Client, &s3.ListObjectsV2Input{Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// DirStore stores objects as files under Root, using the key as a relative path.
type DirStore struct{ Root string }

func (d *DirStore) path(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(key))
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid key %q", key)
	}
	return filepath.Join(d.Root, clean), nil
}

func (d *DirStore) Put(_ context.Context, key string, body []byte, _ string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p) // atomic overwrite, like an S3 PUT
}

func (d *DirStore) Get(_ context.Context, key string) ([]byte, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (d *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(d.Root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if e.IsDir() || strings.HasSuffix(p, ".tmp") {
			return nil
		}
		rel, _ := filepath.Rel(d.Root, p)
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}
