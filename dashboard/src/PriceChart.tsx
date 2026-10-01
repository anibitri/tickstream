import { ColorType, createChart, LineSeries, type IChartApi, type ISeriesApi, type UTCTimestamp } from "lightweight-charts";
import { useEffect, useRef } from "react";

import type { Metrics } from "./api";

export interface LinePoint {
  time: number; // seconds since the epoch
  value: number;
}

// Converts windows to chart points (one per window, at its end time).
export function toPoints(series: Metrics[], field: "last_price" | "vwap"): LinePoint[] {
  return series.map((m) => ({ time: Math.round(m.window_end_ns / 1e9), value: Number(m[field]) }));
}

interface Line {
  label: string;
  color: string;
  points: LinePoint[];
}

// Chart draws one or more lines with TradingView's lightweight-charts.
// Chart is `height` pixels tall, or fills its container when height is omitted.
export function Chart({ lines, height }: { lines: Line[]; height?: number }) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<ISeriesApi<"Line">[]>([]);
  const fitted = useRef(false);

  useEffect(() => {
    if (!box.current) return;
    const c = createChart(box.current, {
      autoSize: true, // follows the box's size, which is fixed by its style
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#9aa4b2" },
      grid: { vertLines: { color: "#1f2733" }, horzLines: { color: "#1f2733" } },
      timeScale: { timeVisible: true, secondsVisible: true },
      rightPriceScale: { borderColor: "#2a3441" },
    });
    chart.current = c;
    return () => {
      c.remove();
      chart.current = null;
      series.current = [];
      fitted.current = false;
    };
  }, [height]);

  useEffect(() => {
    const c = chart.current;
    if (!c) return;
    while (series.current.length < lines.length) {
      const l = lines[series.current.length];
      series.current.push(c.addSeries(LineSeries, { color: l.color, lineWidth: 2, title: l.label }));
    }
    lines.forEach((l, i) => series.current[i].setData(l.points.map((p) => ({ time: p.time as UTCTimestamp, value: p.value }))));
    // Show all the data once when it first arrives; after that, leave the
    // zoom to the user.
    if (!fitted.current && lines.some((l) => l.points.length > 1)) {
      c.timeScale().fitContent();
      fitted.current = true;
    }
  }, [lines]);

  return <div ref={box} className="chart" style={height ? { height } : undefined} />;
}

export function PriceChart({ series, window }: { series: Metrics[]; window: number }) {
  if (series.length === 0) {
    return <div className="empty">Waiting for {window}s windows…</div>;
  }
  return (
    <Chart
      lines={[
        { label: "Last", color: "#5b9cf6", points: toPoints(series, "last_price") },
        { label: "VWAP", color: "#f5a524", points: toPoints(series, "vwap") },
      ]}
    />
  );
}
