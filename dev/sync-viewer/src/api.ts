// Shells SQL queries to the Vite-side `everr telemetry query` proxy.

export async function runQuery<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  const resp = await fetch("/api/query?sql=" + encodeURIComponent(sql.trim()));
  if (!resp.ok) throw new Error(await resp.text());
  const text = await resp.text();
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}
