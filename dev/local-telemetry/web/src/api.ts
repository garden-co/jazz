type SqlEnvelope = {
  columns: string[];
  rows: unknown[][];
};

type SqlErrorEnvelope = { error?: string };

export async function runQuery<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  const resp = await fetch("/sql", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: sql.trim() }),
  });
  const body = await resp.text();
  if (!resp.ok) throw new Error(parseError(body) ?? `sql endpoint returned ${resp.status}`);

  const envelope = JSON.parse(body) as SqlEnvelope;
  return envelope.rows.map((row) => {
    const obj: Record<string, unknown> = {};
    envelope.columns.forEach((column, index) => {
      obj[column] = row[index];
    });
    return obj as T;
  });
}

function parseError(body: string): string | null {
  try {
    const parsed = JSON.parse(body) as SqlErrorEnvelope;
    return typeof parsed.error === "string" ? parsed.error : null;
  } catch {
    return body || null;
  }
}
