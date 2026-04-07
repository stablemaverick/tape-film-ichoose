/**
 * Minimal PostgREST-style chain mock: each awaited query consumes the next
 * `{ data, error }` from `responses` in order (matches loader call sequence).
 */

export type SupabaseResponseRow = { data: unknown; error: null };

export function createSequentialSupabaseMock(responses: SupabaseResponseRow[]) {
  let idx = 0;
  const next = () => {
    const row = responses[idx] ?? { data: [], error: null };
    idx += 1;
    return Promise.resolve(row);
  };

  const makeChain = () => {
    const chain: Record<string, unknown> = {};
    for (const m of [
      "select",
      "or",
      "eq",
      "in",
      "ilike",
      "not",
      "gte",
      "lte",
      "order",
      "limit",
    ]) {
      chain[m] = () => chain;
    }
    chain.then = (onFulfilled: (v: unknown) => unknown, onRejected?: unknown) =>
      next().then(onFulfilled, onRejected as never);
    chain.catch = (onRejected: (e: unknown) => unknown) =>
      next().catch(onRejected);
    return chain;
  };

  return {
    from: (_table: string) => makeChain(),
  };
}
