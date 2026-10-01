import type { Alert, Metrics } from "./api";
import { age, price } from "./format";
import { initialState, MAX_POINTS, reducer, seriesKey } from "./live";

const metric = (end: number, symbol = "BTC-USD", window_secs: 1 | 10 | 60 = 10): Metrics => ({
  symbol,
  window_secs,
  window_start_ns: end - window_secs * 1e9,
  window_end_ns: end,
  vwap: "100",
  volume: "1",
  trade_count: 1,
  realised_vol: 0.5,
  high: "101",
  low: "99",
  last_price: "100.5",
  xex_spread_bps: 1.2,
  emitted_at_ns: end,
  exchanges: [],
});

const alert = (id: string, end: number): Alert => ({
  alert_id: id,
  symbol: "BTC-USD",
  rule_name: "price_jump",
  severity: "WARN",
  window_end_ns: end,
  message: "m",
  values: {},
});

describe("reducer", () => {
  it("appends metrics in time order and ignores repeats", () => {
    let s = reducer(initialState, { type: "metrics", data: metric(2e9) });
    s = reducer(s, { type: "metrics", data: metric(2e9) }); // repeat
    s = reducer(s, { type: "metrics", data: metric(1e9) }); // older
    s = reducer(s, { type: "metrics", data: metric(3e9) });
    expect(s.metrics[seriesKey("BTC-USD", 10)].map((m) => m.window_end_ns)).toEqual([2e9, 3e9]);
  });

  it("keeps a bounded history", () => {
    const items = Array.from({ length: MAX_POINTS + 50 }, (_, i) => metric((i + 1) * 1e9));
    const s = reducer(initialState, { type: "history", symbol: "BTC-USD", window: 10, items });
    const series = s.metrics[seriesKey("BTC-USD", 10)];
    expect(series).toHaveLength(MAX_POINTS);
    expect(series[series.length - 1].window_end_ns).toBe((MAX_POINTS + 50) * 1e9);
  });

  it("orders alerts newest first and drops redelivered ones", () => {
    let s = reducer(initialState, { type: "alerts", items: [alert("a", 1), alert("b", 3)] });
    s = reducer(s, { type: "alert", data: alert("c", 2) });
    s = reducer(s, { type: "alert", data: alert("b", 3) });
    expect(s.alerts.map((a) => a.alert_id)).toEqual(["b", "c", "a"]);
  });

  it("tracks feed health per exchange and symbol", () => {
    const h = { exchange: "kraken", symbol: "BTC-USD", last_trade_ns: 1, reconnects: 0, status: "connected" as const, reported_at_ns: 1 };
    let s = reducer(initialState, { type: "health", data: h });
    s = reducer(s, { type: "health", data: { ...h, status: "stale" } });
    expect(s.health["kraken/BTC-USD"].status).toBe("stale");
  });
});

describe("format", () => {
  it("shows prices with sensible precision", () => {
    expect(price("60123.456789")).toBe("60,123.46");
    expect(price("150.123456")).toBe("150.123");
    expect(price(undefined)).toBe("–");
  });

  it("describes ages", () => {
    const now = 1_000_000;
    expect(age((now - 5_000) * 1e6, now)).toBe("5s ago");
    expect(age((now - 120_000) * 1e6, now)).toBe("2m ago");
    expect(age(0, now)).toBe("never");
  });
});
