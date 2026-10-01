import { useEffect, useState } from "react";

import { api, unwrap } from "./api";
import { num, pct } from "./format";
import { Chart } from "./PriceChart";

// The parts of report.json the page shows (written by cmd/backtester).
interface Confusion {
  precision: number;
  recall: number;
  f1: number;
  true_positives: number;
  false_positives: number;
}
export interface Report {
  run_id: string;
  from: string;
  to: string;
  symbols: string[];
  data: { trades: number; windows: number; alerts: number };
  rules: { name: string; condition: string; out_of_sample_tuned: Confusion; out_of_sample_configured: Confusion }[];
  signal: {
    description: string;
    out_of_sample: {
      trades: number;
      hit_rate: number;
      mean_return_bps: number;
      sharpe_per_trade: number;
      max_drawdown: number;
      total_return: number;
    };
    equity_curve: { time_ns: number; equity: number }[] | null;
  };
}

export function ReportView({ report }: { report: Report }) {
  const s = report.signal.out_of_sample;
  const curve = report.signal.equity_curve ?? [];
  return (
    <div className="report">
      <p className="muted">
        {report.from} → {report.to} · {report.symbols.join(", ")} · {report.data.trades.toLocaleString()} trades ·{" "}
        {report.data.alerts} alerts
      </p>
      <h3>Alert rules (out of sample)</h3>
      <table>
        <thead>
          <tr>
            <th>Rule</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>Precision (configured)</th>
            <th>Recall (configured)</th>
          </tr>
        </thead>
        <tbody>
          {report.rules.map((r) => (
            <tr key={r.name}>
              <td title={r.condition}>{r.name}</td>
              <td>{pct(r.out_of_sample_tuned.precision)}</td>
              <td>{pct(r.out_of_sample_tuned.recall)}</td>
              <td>{pct(r.out_of_sample_configured.precision)}</td>
              <td>{pct(r.out_of_sample_configured.recall)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <h3>Signal (out of sample)</h3>
      <p className="muted">{report.signal.description}</p>
      <div className="stats">
        <Stat label="Trades" value={String(s.trades)} />
        <Stat label="Hit rate" value={pct(s.hit_rate)} />
        <Stat label="Mean return" value={`${num(s.mean_return_bps)} bps`} />
        <Stat label="Sharpe / trade" value={num(s.sharpe_per_trade, 3)} />
        <Stat label="Max drawdown" value={pct(s.max_drawdown, 2)} />
        <Stat label="Total return" value={pct(s.total_return, 2)} />
      </div>
      {curve.length > 1 ? (
        <Chart
          height={240}
          lines={[{ label: "Equity", color: "#3ecf8e", points: curve.map((p) => ({ time: Math.round(p.time_ns / 1e9), value: p.equity })) }]}
        />
      ) : (
        <div className="empty">Not enough trades for an equity curve</div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

export function Backtests() {
  const [runs, setRuns] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [report, setReport] = useState<Report | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    unwrap(api.GET("/api/v1/backtests"))
      .then((r) => {
        setRuns(r.runs);
        if (r.runs.length > 0) setSelected(r.runs[r.runs.length - 1]);
      })
      .catch((e: Error) => setError(`Couldn't load backtests: ${e.message}.`));
  }, []);

  useEffect(() => {
    if (!selected) return;
    unwrap(api.GET("/api/v1/backtests/{run_id}", { params: { path: { run_id: selected } } }))
      .then((r) => setReport(r as unknown as Report))
      .catch((e: Error) => setError(`Couldn't load ${selected}: ${e.message}.`));
  }, [selected]);

  return (
    <section className="panel">
      <header>
        <h2>Backtests</h2>
        {runs.length > 0 && (
          <select value={selected} onChange={(e) => setSelected(e.target.value)} aria-label="Backtest run">
            {runs.map((r) => (
              <option key={r}>{r}</option>
            ))}
          </select>
        )}
      </header>
      {error && <div className="error">{error}</div>}
      {runs.length === 0 && !error && <div className="empty">No backtests yet. Run the backtester against S3 to add one.</div>}
      {report && <ReportView report={report} />}
    </section>
  );
}
