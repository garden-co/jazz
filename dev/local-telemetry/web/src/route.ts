export type SessionRoute = { page: "sessions" } | { page: "session"; sessionId: string };

export function parseSessionRoute(hash: string): SessionRoute {
  const route = hash.replace(/^#/, "") || "/sessions";
  const parts = route.split("/").filter(Boolean);
  if (parts[0] !== "sessions" || !parts[1]) return { page: "sessions" };
  return { page: "session", sessionId: decodeURIComponent(parts.slice(1).join("/")) };
}

export function sessionListHash(): string {
  return "#/sessions";
}

export function sessionDetailHash(sessionId: string): string {
  return `#/sessions/${encodeURIComponent(sessionId)}`;
}
