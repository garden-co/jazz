/** Simple hash router: reads jam ID from /#/<JAM_ID>. This avoids us needing to load a full SvelteKit app */

function parseJamId(): string | null {
  const hash = window.location.hash;
  // Match /#/<id> or #/<id>
  const match = hash.match(/^#\/(.+)$/);
  return match?.[1] ?? null;
}

export function getHashJamId(): string | null {
  return parseJamId();
}

export function setHashJamId(jamId: string): void {
  window.location.hash = `/${jamId}`;
}

export function onHashChange(callback: (jamId: string | null) => void): () => void {
  const handler = () => callback(parseJamId());
  window.addEventListener("hashchange", handler);
  return () => window.removeEventListener("hashchange", handler);
}
