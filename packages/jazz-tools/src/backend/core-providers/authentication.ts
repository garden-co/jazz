import { createHmac } from "node:crypto";
import { accountAppId } from "../../accounts/local-first.js";
import type { JazzCoreSocket } from "./core-provider.js";

export function linkChallengeSigner(appId: string, secret: string): (payload: string) => string {
  const key = createHmac("sha256", secret)
    .update(`jazz-link-challenge-v1\0${accountAppId(appId)}`)
    .digest();
  return (payload) => createHmac("sha256", key).update(payload).digest("base64url");
}

/** Authenticate the first frame using a provider's private credential. */
export function authenticatedSocket(socket: JazzCoreSocket, backendSecret: string): JazzCoreSocket {
  let first = true;
  return {
    send(data) {
      if (first) {
        first = false;
        try {
          const prelude = JSON.parse(new TextDecoder().decode(data));
          const auth = prelude.auth ?? prelude;
          data = new TextEncoder().encode(
            JSON.stringify({
              ...prelude,
              auth: { ...auth, backend_secret: backendSecret },
            }),
          );
        } catch {
          socket.close();
          return false;
        }
      }
      return socket.send(data);
    },
    close: () => socket.close(),
  };
}
