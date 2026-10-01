import { useEffect, useState } from "react";

import { api, unwrap, type WindowSecs } from "./api";
import { Backtests } from "./Backtests";
import { seriesKey, useLive } from "./live";
import { AlertFeed, FeedHealth, MetricsTable } from "./Panels";
import { PriceChart } from "./PriceChart";

const DEFAULT_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"];

export function App() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [symbol, setSymbol] = useState("");
  const [win, setWin] = useState<WindowSecs>(10); // window size in seconds
  const [page, setPage] = useState<"live" | "backtests">(location.pathname.startsWith("/backtests") ? "backtests" : "live");
  const live = useLive(symbols);

  useEffect(() => {
    unwrap(api.GET("/api/v1/symbols"))
      .then((list) => list.map((s) => s.symbol))
      .catch(() => DEFAULT_SYMBOLS)
      .then((list) => {
        setSymbols(list);
        setSymbol(list[0] ?? "");
      });
  }, []);

  const go = (p: "live" | "backtests") => {
    history.pushState(null, "", p === "live" ? "/" : "/backtests");
    setPage(p);
  };

  const feeds = Object.values(live.health)
    .filter((h) => h.symbol === symbol)
    .sort((a, b) => a.exchange.localeCompare(b.exchange));

  return (
    <div className="app">
      <header className="top">
        <h1>Tickstream</h1>
        <nav>
          <button className={page === "live" ? "on" : ""} onClick={() => go("live")}>
            Live
          </button>
          <button className={page === "backtests" ? "on" : ""} onClick={() => go("backtests")}>
            Backtests
          </button>
        </nav>
        <span className={`conn ${live.connected ? "up" : "down"}`}>{live.connected ? "live" : "reconnecting…"}</span>
      </header>

      {page === "backtests" ? (
        <Backtests />
      ) : (
        <main className="grid">
          <section className="panel chart-panel">
            <header>
              <div className="tabs" role="tablist">
                {symbols.map((s) => (
                  <button key={s} role="tab" aria-selected={s === symbol} className={s === symbol ? "on" : ""} onClick={() => setSymbol(s)}>
                    {s}
                  </button>
                ))}
              </div>
              <div className="tabs">
                {([1, 10, 60] as const).map((w) => (
                  <button key={w} className={w === win ? "on" : ""} onClick={() => setWin(w)}>
                    {w}s
                  </button>
                ))}
              </div>
            </header>
            <PriceChart series={live.metrics[seriesKey(symbol, win)] ?? []} window={win} />
          </section>

          <section className="panel health-panel">
            <header>
              <h2>Feeds · {symbol}</h2>
            </header>
            <FeedHealth feeds={feeds} />
          </section>

          <section className="panel table-panel">
            <header>
              <h2>Latest {win}s window</h2>
            </header>
            <MetricsTable
              rows={symbols.map((s) => {
                const series = live.metrics[seriesKey(s, win)] ?? [];
                return { symbol: s, latest: series[series.length - 1] };
              })}
            />
          </section>

          <section className="panel alerts-panel">
            <header>
              <h2>Alerts</h2>
            </header>
            <AlertFeed alerts={live.alerts} symbol={symbol} />
          </section>
        </main>
      )}
    </div>
  );
}
