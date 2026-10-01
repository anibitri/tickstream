// Small display helpers.

// Prices arrive as exact decimal strings; show a sensible number of digits.
export function price(s: string | undefined): string {
  if (s === undefined || s === "") return "–";
  const n = Number(s);
  const digits = n >= 1000 ? 2 : n >= 1 ? 3 : 6;
  return n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function num(n: number | undefined, digits = 2): string {
  if (n === undefined || Number.isNaN(n)) return "–";
  return n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function pct(n: number | undefined, digits = 1): string {
  return n === undefined ? "–" : `${num(n * 100, digits)}%`;
}

// Time of day (UTC) for a nanosecond timestamp.
export function clock(ns: number): string {
  return new Date(ns / 1e6).toISOString().slice(11, 19);
}

// "12s ago" style age of a nanosecond timestamp.
export function age(ns: number, nowMs = Date.now()): string {
  if (!ns) return "never";
  const s = Math.max(0, Math.round((nowMs - ns / 1e6) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}
