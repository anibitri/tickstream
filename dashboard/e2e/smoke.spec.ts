import { expect, test } from "@playwright/test";

const now = Date.now() * 1e6;
const metrics = (end: number) => ({
  symbol: "BTC-USD",
  window_secs: 10,
  window_start_ns: end - 10e9,
  window_end_ns: end,
  vwap: "60000.5",
  volume: "1.25",
  trade_count: 42,
  realised_vol: 0.61,
  high: "60010",
  low: "59990",
  last_price: "60001",
  xex_spread_bps: 3.4,
  emitted_at_ns: end,
  exchanges: [],
});

test("live page shows metrics, feeds and a pushed alert", async ({ page }) => {
  await page.route("**/api/v1/symbols", (r) =>
    r.fulfill({
      json: [
        {
          symbol: "BTC-USD",
          feeds: [{ exchange: "coinbase", symbol: "BTC-USD", last_trade_ns: now, reconnects: 0, status: "connected", reported_at_ns: now }],
        },
      ],
    }),
  );
  await page.route("**/api/v1/metrics/*/history**", (r) => r.fulfill({ json: [metrics(now - 20e9), metrics(now - 10e9)] }));
  await page.route("**/api/v1/alerts**", (r) => r.fulfill({ json: { items: [] } }));
  await page.routeWebSocket(/\/ws/, (ws) => {
    ws.send(
      JSON.stringify({
        type: "alert",
        data: { alert_id: "x", symbol: "BTC-USD", rule_name: "price_jump", severity: "WARN", window_end_ns: now, message: "BTC jumped", values: {} },
      }),
    );
  });

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Tickstream" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "BTC-USD" })).toBeVisible();
  await expect(page.getByText("60,001.00")).toBeVisible(); // latest 10s window in the table
  await expect(page.getByText("coinbase")).toBeVisible();
  await expect(page.getByText("BTC jumped")).toBeVisible();
  await expect(page.getByText("live", { exact: true })).toBeVisible();
});
