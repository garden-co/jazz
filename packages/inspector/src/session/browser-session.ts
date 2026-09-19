export type InspectorCapability = "inspector:read" | "inspector:edit" | "inspector:admin";
export interface InspectorSession {
  accessToken: string;
  expiresAt: number;
  appId: string;
  serverUrl: string;
  capabilities: InspectorCapability[];
}

const capabilities = new Set(["inspector:read", "inspector:edit", "inspector:admin"]);
export function validateSession(value: unknown, appId: string, now = Date.now()): InspectorSession {
  const session = value as Partial<InspectorSession> | null;
  if (
    !session ||
    typeof session.accessToken !== "string" ||
    !session.accessToken ||
    session.appId !== appId ||
    typeof session.expiresAt !== "number" ||
    !Number.isSafeInteger(session.expiresAt) ||
    session.expiresAt * 1000 <= now ||
    session.expiresAt * 1000 > now + 900_000 ||
    !Array.isArray(session.capabilities) ||
    !session.capabilities.includes("inspector:read") ||
    session.capabilities.some((c) => !capabilities.has(c)) ||
    typeof session.serverUrl !== "string"
  ) {
    throw new Error("Invalid Inspector session");
  }
  const server = new URL(session.serverUrl);
  if (
    (server.protocol !== "https:" &&
      !(
        server.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(server.hostname)
      )) ||
    server.username ||
    server.password ||
    server.search ||
    server.hash
  )
    throw new Error("Invalid Inspector server");
  return {
    accessToken: session.accessToken,
    expiresAt: session.expiresAt,
    appId,
    serverUrl: session.serverUrl,
    capabilities: [...session.capabilities],
  };
}

function base64url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
export function randomProof(): string {
  return base64url(crypto.getRandomValues(new Uint8Array(32)));
}
export async function proofChallenge(verifier: string): Promise<string> {
  return base64url(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier))),
  );
}

/** Deployment configuration, never a callback-supplied authority. */
export function dashboardOrigin(value: string): string {
  const url = new URL(value);
  if (
    url.protocol !== "https:" &&
    !(url.protocol === "http:" && ["localhost", "127.0.0.1"].includes(url.hostname))
  )
    throw new Error("Dashboard must use HTTPS");
  if (url.username || url.password || url.pathname !== "/" || url.search || url.hash)
    throw new Error("Dashboard must be an exact configured origin");
  return url.origin;
}

export class DashboardInspectorSession {
  private generation = 0;
  private popup: Window | null = null;
  private cancelPending: (() => void) | null = null;
  readonly origin: string;
  readonly appId: string;
  readonly redirectUri: string;
  constructor(origin: string, appId: string, redirectUri: string) {
    this.appId = appId;
    this.redirectUri = redirectUri;
    this.origin = dashboardOrigin(origin);
    const callback = new URL(redirectUri);
    if (callback.origin !== window.location.origin || callback.search || callback.hash)
      throw new Error("Invalid Inspector callback");
  }

  logout(): void {
    this.generation++;
    this.cancelPending?.();
    this.cancelPending = null;
    this.popup?.close();
    this.popup = null;
  }

  /** Called synchronously by a user gesture so the blank popup is not blocked. */
  async authorize(
    requested: InspectorCapability[] = ["inspector:read"],
  ): Promise<InspectorSession> {
    this.logout();
    const generation = this.generation;
    const popup = window.open("about:blank", "jazz-inspector-login", "popup,width=520,height=700");
    if (!popup) throw new Error("Allow the login popup, then try again");
    this.popup = popup;
    try {
      const state = randomProof();
      const verifier = randomProof();
      const challenge = await proofChallenge(verifier);
      if (this.generation !== generation) {
        popup.close();
        throw new Error("Inspector login cancelled");
      }
      const authorize = new URL("/inspector/authorize", this.origin);
      authorize.search = new URLSearchParams({
        app_id: this.appId,
        redirect_uri: this.redirectUri,
        state,
        code_challenge: challenge,
        code_challenge_method: "S256",
        capabilities: requested.join(" "),
      }).toString();
      const code = await new Promise<string>((resolve, reject) => {
        const finish = (code?: string) => {
          clearTimeout(timeout);
          clearInterval(closed);
          window.removeEventListener("message", receive);
          this.cancelPending = null;
          if (code) resolve(code);
          else reject(new Error("Inspector login cancelled or expired"));
        };
        const receive = (event: MessageEvent) => {
          if (event.origin !== new URL(this.redirectUri).origin || event.source !== popup) return;
          if (
            event.data?.type !== "jazz-inspector-code" ||
            event.data.state !== state ||
            typeof event.data.code !== "string"
          )
            return;
          finish(event.data.code);
        };
        const timeout = setTimeout(() => finish(), 60_000);
        const closed = setInterval(() => {
          if (popup.closed) finish();
        }, 500);
        this.cancelPending = () => finish();
        window.addEventListener("message", receive);
        popup.location.replace(authorize.href);
      });
      const response = await fetch(new URL("/inspector/token", this.origin), {
        method: "POST",
        credentials: "omit",
        cache: "no-store",
        redirect: "error",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code, code_verifier: verifier, redirect_uri: this.redirectUri }),
      });
      if (!response.ok) throw new Error("Inspector login denied");
      const session = validateSession(await response.json(), this.appId);
      if (session.capabilities.some((capability) => !requested.includes(capability)))
        throw new Error("Unexpected Inspector capability");
      if (this.generation !== generation) throw new Error("Inspector login cancelled");
      return session;
    } finally {
      popup.close();
      if (this.popup === popup) this.popup = null;
    }
  }

  /** Same-site cookie renewal; failures require an explicit new login gesture. */
  async renew(requested: InspectorCapability[] = ["inspector:read"]): Promise<InspectorSession> {
    const generation = this.generation;
    const csrfResponse = await fetch(new URL("/inspector/session", this.origin), {
      credentials: "include",
      cache: "no-store",
      redirect: "error",
    });
    if (!csrfResponse.ok) throw new Error("Inspector session ended. Sign in again.");
    const csrf = (await csrfResponse.json()) as { csrfToken?: unknown };
    if (typeof csrf.csrfToken !== "string" || !csrf.csrfToken)
      throw new Error("Inspector session ended");
    if (this.generation !== generation) throw new Error("Inspector session ended");
    const response = await fetch(new URL("/inspector/renew", this.origin), {
      method: "POST",
      credentials: "include",
      cache: "no-store",
      redirect: "error",
      // Custom header + JSON force preflight; server also verifies exact Origin.
      headers: { "Content-Type": "application/json", "X-Inspector-CSRF": csrf.csrfToken },
      body: JSON.stringify({ appId: this.appId, capabilities: requested }),
    });
    if (!response.ok) throw new Error("Inspector session ended. Sign in again.");
    const session = validateSession(await response.json(), this.appId);
    if (session.capabilities.some((capability) => !requested.includes(capability)))
      throw new Error("Unexpected Inspector capability");
    if (this.generation !== generation) throw new Error("Inspector session ended");
    return session;
  }
}

/** Callback contains a one-time code only; remove it before rendering anything. */
export function completeInspectorCallback(): boolean {
  if (window.location.pathname !== "/inspector/callback") return false;
  const params = new URLSearchParams(window.location.search);
  window.history.replaceState(null, "", window.location.pathname);
  const code = params.get("code"),
    state = params.get("state");
  if (code && state && window.opener)
    window.opener.postMessage({ type: "jazz-inspector-code", code, state }, window.location.origin);
  return true;
}

export async function receiveCliSession(
  handoff: string,
  appId: string,
  launchCode: string,
): Promise<InspectorSession> {
  if (!/^[A-Za-z0-9_-]{43}$/.test(launchCode)) throw new Error("Invalid Inspector launch code");
  const endpoint = new URL(handoff);
  if (
    endpoint.protocol !== "http:" ||
    endpoint.hostname !== "127.0.0.1" ||
    !endpoint.port ||
    endpoint.pathname !== "/" ||
    endpoint.search ||
    endpoint.hash ||
    endpoint.username ||
    endpoint.password
  )
    throw new Error("Invalid loopback handoff");
  const verifier = randomProof();
  const post = (path: string, body: unknown) =>
    fetch(new URL(path, endpoint), {
      method: "POST",
      credentials: "omit",
      cache: "no-store",
      redirect: "error",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  const challenge = await post("/challenge", {
    launch_code: launchCode,
    code_challenge: await proofChallenge(verifier),
  });
  if (!challenge.ok) throw new Error("Inspector handoff expired. Run the CLI again.");
  const { code } = (await challenge.json()) as { code?: unknown };
  if (typeof code !== "string" || !code) throw new Error("Invalid Inspector handoff");
  const response = await post("/token", { code, code_verifier: verifier });
  if (!response.ok) throw new Error("Inspector handoff denied");
  return validateSession(await response.json(), appId);
}
