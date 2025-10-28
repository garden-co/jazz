export function isCoValueId(id: unknown): id is `co_z${string}` {
  return typeof id === "string" && id.startsWith("co_z");
}
