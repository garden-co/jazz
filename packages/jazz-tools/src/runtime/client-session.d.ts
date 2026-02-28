import type { LocalAuthMode, Session } from "./context.js";
interface ClientSessionInput {
  appId: string;
  jwtToken?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
}
export declare function deriveLocalPrincipalId(
  appId: string,
  mode: LocalAuthMode,
  token: string,
): Promise<string>;
/**
 * Resolve the client session that will be used for permission checks.
 *
 * Priority mirrors request auth headers:
 * 1. JWT (Authorization bearer token)
 * 2. Local anonymous/demo auth (mode + token)
 * 3. No session
 */
export declare function resolveClientSession(config: ClientSessionInput): Promise<Session | null>;

//# sourceMappingURL=client-session.d.ts.map
