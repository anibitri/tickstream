import { defineConfig } from "@playwright/test";

// Smoke test: builds the dashboard, serves it with `vite preview`, and fakes
// the API and websocket inside the test, so no backend is needed.
export default defineConfig({
  testDir: "e2e",
  use: { baseURL: "http://localhost:4173" },
  webServer: {
    command: "npm run build && npx vite preview --port 4173 --strictPort",
    port: 4173,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
