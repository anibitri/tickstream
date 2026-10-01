// Live data: initial history from the REST API, then updates pushed over the
// websocket. All state changes go through one pure reducer, which keeps the
// logic easy to test.
import { useEffect, useReducer } from "react";

import { api, unwrap, type Alert, type FeedHealth, type Metrics, type WindowSecs } from "./api";

export const MAX_POINTS = 600; // windows kept per symbol and window size
export const MAX_ALERTS = 200;

export interface LiveState {
  metrics: Record<string, Metrics[]>; // key: "BTC-USD/10", oldest first
  alerts: Alert[]; // newest first
  health: Record<string, FeedHealth>; // key: "kraken/BTC-USD"
  connected: boolean;
}

export type Action =
  | { type: "history"; symbol: string; window: WindowSecs; items: Metrics[] }
  | { type: "metrics"; data: Metrics }
  | { type: "alerts"; items: Alert[] }
  | { type: "alert"; data: Alert }
  | { type: "health"; data: FeedHealth }
  | { type: "connected"; value: boolean };

export const initialState: LiveState = { metrics: {}, alerts: [], health: {}, connected: false };

export const seriesKey = (symbol: string, window: number) => `${symbol}/${window}`;

// Adds m to a series, ignoring repeats and anything older than the last point.
function appendMetric(series: Metrics[] = [], m: Metrics): Metrics[] {
  const last = series[series.length - 1];
  if (last && last.window_end_ns >= m.window_end_ns) return series;
  const next = [...series, m];
  return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
}

function addAlert(alerts: Alert[], a: Alert): Alert[] {
  if (alerts.some((x) => x.alert_id === a.alert_id)) return alerts; // redelivered
  const next = [a, ...alerts].sort((x, y) => y.window_end_ns - x.window_end_ns);
  return next.slice(0, MAX_ALERTS);
}

export function reducer(state: LiveState, action: Action): LiveState {
  switch (action.type) {
    case "history": {
      const key = seriesKey(action.symbol, action.window);
      let series = state.metrics[key] ?? [];
      for (const m of action.items) series = appendMetric(series, m);
      return { ...state, metrics: { ...state.metrics, [key]: series } };
    }
    case "metrics": {
      const key = seriesKey(action.data.symbol, action.data.window_secs);
      return { ...state, metrics: { ...state.metrics, [key]: appendMetric(state.metrics[key], action.data) } };
    }
    case "alerts":
      return { ...state, alerts: action.items.reduce(addAlert, state.alerts) };
    case "alert":
      return { ...state, alerts: addAlert(state.alerts, action.data) };
    case "health":
      return { ...state, health: { ...state.health, [`${action.data.exchange}/${action.data.symbol}`]: action.data } };
    case "connected":
      return { ...state, connected: action.value };
  }
}

// useLive loads recent history for the given symbols, then keeps a websocket
// open (reconnecting with a growing delay) and feeds every message into the reducer.
export function useLive(symbols: string[]): LiveState {
  const [state, dispatch] = useReducer(reducer, initialState);
  const key = symbols.join(",");

  useEffect(() => {
    if (!key) return;
    let cancelled = false;
    let socket: WebSocket | undefined;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;

    const loadHistory = async () => {
      for (const symbol of symbols) {
        for (const window of [1, 10, 60] as const) {
          try {
            const items = await unwrap(
              api.GET("/api/v1/metrics/{symbol}/history", {
                params: { path: { symbol }, query: { window, limit: 300 } },
              }),
            );
            if (!cancelled) dispatch({ type: "history", symbol, window, items });
          } catch {
            /* the chart simply starts empty */
          }
        }
      }
      try {
        const page = await unwrap(api.GET("/api/v1/alerts", { params: { query: { limit: 100 } } }));
        if (!cancelled) dispatch({ type: "alerts", items: page.items });
        const syms = await unwrap(api.GET("/api/v1/symbols"));
        for (const s of syms) for (const f of s.feeds) if (!cancelled) dispatch({ type: "health", data: f });
      } catch {
        /* shown as empty panels */
      }
    };

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${proto}://${location.host}/ws?symbols=${encodeURIComponent(key)}`);
      socket.onopen = () => {
        attempt = 0;
        dispatch({ type: "connected", value: true });
      };
      socket.onmessage = (ev: MessageEvent<string>) => {
        const msg = JSON.parse(ev.data) as { type: "metrics" | "alert" | "health"; data: never };
        dispatch({ type: msg.type, data: msg.data });
      };
      socket.onclose = () => {
        dispatch({ type: "connected", value: false });
        if (cancelled) return;
        const delay = Math.min(30_000, 500 * 2 ** attempt++) * Math.random();
        retry = setTimeout(connect, delay);
      };
    };

    void loadHistory();
    connect();
    return () => {
      cancelled = true;
      clearTimeout(retry);
      socket?.close();
    };
  }, [key]); // `key` changes exactly when the symbol list does


  return state;
}
