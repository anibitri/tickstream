import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import type { Alert } from "./api";
import { ReportView, type Report } from "./Backtests";
import { AlertFeed, FeedHealth, MetricsTable } from "./Panels";

const alerts: Alert[] = [
  { alert_id: "1", symbol: "BTC-USD", rule_name: "cross_exchange_divergence", severity: "CRITICAL", window_end_ns: 3e18, message: "big spread", values: {} },
  { alert_id: "2", symbol: "ETH-USD", rule_name: "price_jump", severity: "WARN", window_end_ns: 2e18, message: "eth jump", values: {} },
];

describe("AlertFeed", () => {
  it("filters by severity and symbol", async () => {
    render(<AlertFeed alerts={alerts} symbol="BTC-USD" />);
    expect(screen.getByText("big spread")).toBeTruthy();
    expect(screen.getByText("eth jump")).toBeTruthy();

    await userEvent.click(screen.getByRole("button", { name: "WARN" }));
    expect(screen.queryByText("eth jump")).toBeNull();

    await userEvent.click(screen.getByRole("button", { name: "WARN" }));
    await userEvent.click(screen.getByRole("checkbox"));
    expect(screen.queryByText("eth jump")).toBeNull();
    expect(screen.getByText("big spread")).toBeTruthy();
  });

  it("says when there is nothing to show", () => {
    render(<AlertFeed alerts={[]} symbol="BTC-USD" />);
    expect(screen.getByText("No alerts")).toBeTruthy();
  });
});

describe("FeedHealth", () => {
  it("shows status, age and reconnects", () => {
    const now = 10_000;
    render(
      <FeedHealth
        now={now}
        feeds={[{ exchange: "kraken", symbol: "BTC-USD", last_trade_ns: (now - 3000) * 1e6, reconnects: 2, status: "stale", reported_at_ns: 1 }]}
      />,
    );
    expect(screen.getByText("stale")).toBeTruthy();
    expect(screen.getByText("3s ago")).toBeTruthy();
    expect(screen.getByText("2 reconnects")).toBeTruthy();
  });
});

describe("MetricsTable", () => {
  it("shows a dash for symbols without data", () => {
    render(<MetricsTable rows={[{ symbol: "SOL-USD" }]} />);
    expect(screen.getByText("SOL-USD")).toBeTruthy();
    expect(screen.getAllByText("–").length).toBeGreaterThan(3);
  });
});

describe("ReportView", () => {
  it("renders rule scores and signal stats", () => {
    const c = { precision: 0.5, recall: 0.25, f1: 0.33, true_positives: 1, false_positives: 1 };
    const report: Report = {
      run_id: "r1",
      from: "2026-10-04T13:50:00Z",
      to: "2026-10-04T14:10:00Z",
      symbols: ["BTC-USD"],
      data: { trades: 1000, windows: 10, alerts: 2 },
      rules: [{ name: "price_jump", condition: "abs(zscore(last_price, lookback=30)) > 4", out_of_sample_tuned: c, out_of_sample_configured: c }],
      signal: {
        description: "mean reversion",
        out_of_sample: { trades: 4, hit_rate: 0.5, mean_return_bps: -3, sharpe_per_trade: -0.1, max_drawdown: 0.01, total_return: -0.002 },
        equity_curve: null,
      },
    };
    render(<ReportView report={report} />);
    expect(screen.getByText("price_jump")).toBeTruthy();
    expect(screen.getAllByText("50.0%").length).toBe(3); // precision twice + hit rate
    expect(screen.getByText("Not enough trades for an equity curve")).toBeTruthy();
  });
});
