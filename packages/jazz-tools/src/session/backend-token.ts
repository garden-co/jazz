import { resolveServerUrl } from "../runtime/url.js";

/**
 * A `getToken` for an app that mounts Jazz in its own backend with
 * `createJazz`. It asks the backend's `<basePath>/session` route, which
 * signs a short-lived credential for whoever your backend says is signed
 * in. Same-origin, so your app's own session cookie is all it needs.
 *
 * ```ts
 * await jazz.loginOrRegisterJWT({ getToken: backendToken() });
 * ```
 */
export function backendToken(options: { basePath?: string } = {}): () => Promise<string> {
  const base = (options.basePath ?? "/jazz").replace(/\/+$/, "");
  return async () => {
    const response = await fetch(resolveServerUrl(`${base}/session`), {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
    if (response.status === 401) throw new Error("Not signed in to this app's backend");
    if (!response.ok) throw new Error(`Jazz session request failed (${response.status})`);
    const { token } = (await response.json()) as { token: string };
    return token;
  };
}
