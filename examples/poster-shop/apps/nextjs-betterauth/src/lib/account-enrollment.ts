/** An invite as carried in a link's URL fragment: never a path or query value. */
export type InviteLink = { canvasId: string; token: string };

const INVITE_FRAGMENT = /^#invite\/([0-9a-f-]{36})\/([0-9a-f-]{36})$/;

/**
 * Invite links look like `/dashboard#invite/<canvasId>/<token>`. The fragment
 * never reaches the server, so the token stays out of access logs, CDN logs
 * and `Referer` headers; the client posts it in a request body instead.
 */
export function inviteLinkFor(origin: string, invite: InviteLink): string {
  return `${origin}/dashboard#invite/${invite.canvasId}/${invite.token}`;
}

export function parseInviteFragment(hash: string): InviteLink | null {
  const match = INVITE_FRAGMENT.exec(hash);
  return match ? { canvasId: match[1]!, token: match[2]! } : null;
}

export async function bootstrapPersonalCanvas(token: string): Promise<Response> {
  return await fetch("/api/bootstrap", {
    method: "POST",
    credentials: "same-origin",
    headers: { authorization: `Bearer ${token}` },
  });
}

export async function joinCanvasWithInvite(token: string, invite: InviteLink): Promise<Response> {
  return await fetch("/api/join", {
    method: "POST",
    credentials: "same-origin",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(invite),
  });
}

export type StudioPreparation =
  | {
      ok: true;
      joinedCanvasId: string | null;
      /** The link was unknown, already used or revoked: open the user's own studio. */
      inviteRejected: boolean;
    }
  | { ok: false };

// One preparation per user and invite at a time: React StrictMode mounts the
// dashboard effect twice, and both mounts share this promise instead of
// sending two bootstrap requests.
const inFlight = new Map<string, Promise<StudioPreparation>>();

const bootstrappedKey = (userId: string) => `poster-shop:bootstrapped:${userId}`;

function wasBootstrapped(userId: string): boolean {
  try {
    return localStorage.getItem(bootstrappedKey(userId)) === "1";
  } catch {
    return false;
  }
}

function rememberBootstrapped(userId: string) {
  try {
    localStorage.setItem(bootstrappedKey(userId), "1");
  } catch {
    // Storage unavailable: the next load asks the (idempotent) server again.
  }
}

/** Whether the studio has anything left to ask the server for. */
export function needsPreparation(userId: string, invite: InviteLink | null): boolean {
  return !!invite || !wasBootstrapped(userId);
}

/**
 * Creates the user's first poster (once per user: the browser remembers it
 * did) and redeems an invite link. The studio renders from local data while
 * this runs. The bootstrap goes first because it seeds a poster only for a
 * user with no membership yet; joining first would skip the personal poster.
 */
export function prepareStudio(
  userId: string,
  getToken: () => Promise<string | null>,
  invite: InviteLink | null,
): Promise<StudioPreparation> {
  const key = `${userId}:${invite?.token ?? ""}`;
  let pending = inFlight.get(key);
  if (!pending) {
    pending = (async (): Promise<StudioPreparation> => {
      if (!needsPreparation(userId, invite))
        return { ok: true, joinedCanvasId: null, inviteRejected: false };
      const token = await getToken();
      if (!token) return { ok: false };
      if (!wasBootstrapped(userId)) {
        const bootstrapped = await bootstrapPersonalCanvas(token);
        if (!bootstrapped.ok) return { ok: false };
        rememberBootstrapped(userId);
      }
      if (!invite) return { ok: true, joinedCanvasId: null, inviteRejected: false };
      const joined = await joinCanvasWithInvite(token, invite);
      // 400/404 are final answers about the link; retrying cannot help.
      if (joined.status === 400 || joined.status === 404)
        return { ok: true, joinedCanvasId: null, inviteRejected: true };
      if (!joined.ok) return { ok: false };
      const { canvasId } = (await joined.json()) as { canvasId: string };
      return { ok: true, joinedCanvasId: canvasId, inviteRejected: false };
    })()
      .catch((): StudioPreparation => ({ ok: false }))
      .finally(() => inFlight.delete(key));
    inFlight.set(key, pending);
  }
  return pending;
}
