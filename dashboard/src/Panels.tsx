import { useState } from "react";

import type { Alert, FeedHealth as Health, Metrics } from "./api";
import { age, clock, num, pct, price } from "./format";

// MetricsTable shows the latest window of each symbol.
export function MetricsTable({ rows }: { rows: { symbol: string; latest?: Metrics }[] }) {
  return (
    <table className="metrics">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Last</th>
          <th>VWAP</th>
          <th>High / Low</th>
          <th>Volume</th>
          <th>Trades</th>
          <th title="Annualised realised volatility">Vol</th>
          <th title="Coinbase VWAP minus Kraken VWAP">Spread (bps)</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(({ symbol, latest: m }) => (
          <tr key={symbol}>
            <td className="sym">{symbol}</td>
            <td>{price(m?.last_price)}</td>
            <td>{price(m?.vwap)}</td>
            <td>
              {price(m?.high)} / {price(m?.low)}
            </td>
            <td>{m ? num(Number(m.volume), 4) : "–"}</td>
            <td>{m?.trade_count ?? "–"}</td>
            <td>{pct(m?.realised_vol)}</td>
            <td className={m && Math.abs(m.xex_spread_bps) > 10 ? "warn" : ""}>{num(m?.xex_spread_bps)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const SEVERITIES = ["CRITICAL", "WARN", "INFO"] as const;

// AlertFeed lists alerts newest first, filterable by severity and symbol.
export function AlertFeed({ alerts, symbol }: { alerts: Alert[]; symbol: string }) {
  const [shown, setShown] = useState<Set<string>>(new Set(SEVERITIES));
  const [onlySymbol, setOnlySymbol] = useState(false);
  const visible = alerts.filter((a) => shown.has(a.severity) && (!onlySymbol || a.symbol === symbol));

  const toggle = (s: string) => {
    const next = new Set(shown);
    if (next.has(s)) next.delete(s);
    else next.add(s);
    setShown(next);
  };

  return (
    <div className="alerts">
      <div className="filters">
        {SEVERITIES.map((s) => (
          <button key={s} className={`chip ${s.toLowerCase()} ${shown.has(s) ? "on" : ""}`} onClick={() => toggle(s)}>
            {s}
          </button>
        ))}
        <label>
          <input type="checkbox" checked={onlySymbol} onChange={(e) => setOnlySymbol(e.target.checked)} /> only {symbol}
        </label>
      </div>
      {visible.length === 0 ? (
        <div className="empty">No alerts</div>
      ) : (
        <ul>
          {visible.map((a) => (
            <li key={a.alert_id} className={a.severity.toLowerCase()}>
              <span className="time">{clock(a.window_end_ns)}</span>
              <span className="badge">{a.severity}</span>
              <span className="msg">{a.message}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// FeedHealth shows one row per exchange feed of the selected symbol.
export function FeedHealth({ feeds, now }: { feeds: Health[]; now?: number }) {
  if (feeds.length === 0) return <div className="empty">No feed reports yet</div>;
  return (
    <table className="health">
      <tbody>
        {feeds.map((f) => (
          <tr key={f.exchange}>
            <td>
              <span className={`dot ${f.status}`} /> {f.exchange}
            </td>
            <td>{f.status}</td>
            <td title="Time since the last trade">{age(f.last_trade_ns, now)}</td>
            <td title="Reconnects since the ingestor started">{f.reconnects} reconnects</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
