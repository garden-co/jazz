import { randomBytes } from "node:crypto";
import { JazzCore as NativeJazzCore } from "jazz-napi";
import type { JazzCore } from "./core-provider.js";
import { authenticatedSocket, linkChallengeSigner } from "./authentication.js";
/** Open the native core; createJazz binds each session to its bundle. */
export async function openInProcessCore(options: {
  appId: string;
  dataDir?: string;
  inMemory?: boolean;
  blobs?: "files" | "s3" | "sqlite";
  s3Bucket?: string;
  s3Region?: string;
  s3Endpoint?: string;
  s3Prefix?: string;
  blobsDir?: string;
}): Promise<JazzCore> {
  const backendSecret = randomBytes(32).toString("base64url");
  const adminSecret = randomBytes(32).toString("base64url");
  const { appId } = options;
  const core = await NativeJazzCore.open({ ...options, backendSecret, adminSecret });
  const opened: JazzCore = {
    appId,
    signLinkChallenge: linkChallengeSigner(appId, adminSecret),
    async handle(method, path, headers, body, access = "session") {
      const authenticated = [...headers];
      if (access === "admin") authenticated.push(["x-jazz-admin-secret", adminSecret]);
      if (access === "backend") authenticated.push(["x-jazz-backend-secret", backendSecret]);
      return core.handle(method, path, authenticated, Buffer.from(body));
    },
    openSocket: (headers, onEvent) =>
      authenticatedSocket(
        core.openSocket(headers, (error, event) => {
          if (error) onEvent(error, { kind: "close", code: 1011, reason: error.message });
          else if (event.kind === "message" || event.kind === "close")
            onEvent(null, { ...event, kind: event.kind });
        }),
        backendSecret,
      ),
    async close() {
      await core.close();
    },
  };
  return opened;
}
