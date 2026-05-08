export function traceRowPayloadLabel(attrs: Record<string, string>): string {
  if (attrs.payload) return attrs.payload;

  const fields = attrs["jazz.span.fields"];
  if (!fields) return "";

  try {
    const parsed = JSON.parse(fields) as Record<string, unknown>;
    return typeof parsed.payload === "string" ? parsed.payload : "";
  } catch {
    return "";
  }
}
