export async function runQuery<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  const resp = await fetch("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql: sql.trim() }),
  });
  if (!resp.ok) throw new Error(await resp.text());
  const text = await resp.text();
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}
