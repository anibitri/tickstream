// Typed REST client. The types in schema.ts are generated from the
// api-gateway's OpenAPI spec (`npm run gen:api`).
import createClient from "openapi-fetch";

import type { components, paths } from "./schema";

export type Metrics = components["schemas"]["Metrics"];
export type Alert = components["schemas"]["Alert"];
export type FeedHealth = components["schemas"]["FeedHealth"];
export type SymbolInfo = components["schemas"]["SymbolInfo"];
export type WindowSecs = 1 | 10 | 60;

export const api = createClient<paths>({ baseUrl: "" });

// Throws with a readable message instead of returning undefined data.
export async function unwrap<T>(req: Promise<{ data?: T; error?: unknown; response: Response }>): Promise<T> {
  const { data, error, response } = await req;
  if (data === undefined) {
    const detail = typeof error === "object" && error !== null && "error" in error ? String(error.error) : "";
    throw new Error(detail || (response.status >= 500 ? "the API is not reachable" : `the API returned ${response.status}`));
  }
  return data;
}
