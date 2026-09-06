import type {
  DbConfig as PackageDbConfig,
  InsertOptions as PackageInsertOptions,
  PublicSession,
} from "../index.js";
import type {
  DbConfig as RuntimeDbConfig,
  InsertOptions as RuntimeInsertOptions,
} from "./index.js";
import { userIdentity } from "../index.js";

// @ts-expect-error Internal app contexts can carry server-only credentials.
import type { AppContext } from "../index.js";
void (null as unknown as AppContext);

// @ts-expect-error Internal transport sessions are not a public package type.
import type { Session } from "../index.js";
declare const unavailableSession: Session;
void unavailableSession;

// @ts-expect-error sessionAuthor was replaced by userIdentity without a compatibility alias.
import { sessionAuthor } from "../index.js";
void sessionAuthor;

// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as PackageCreateOptions } from "../index.js";
// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as RuntimeCreateOptions } from "./index.js";

const packageInsertOptions: PackageInsertOptions = { id: "row-1", updatedAt: 1 };
const runtimeInsertOptions: RuntimeInsertOptions = { id: "row-1", updatedAt: 1 };
const cookieSession = {
  issuer: "https://issuer.example",
  user_id: "user",
  claims: {},
  authMode: "external" as const,
};
declare const publicSession: PublicSession;
publicSession.user.account satisfies string | null;
publicSession.user.identity.issuer satisfies string;
publicSession.user.identity.subject satisfies string;
// @ts-expect-error Raw transport issuer is not exposed by PublicSession.
void publicSession.issuer;
// @ts-expect-error Raw transport subject is not exposed by PublicSession.
void publicSession.user_id;
userIdentity("https://issuer.example", "user") satisfies string;

declare const publicDb: import("../index.js").Db;
// @ts-expect-error Framework transport session access is package-private.
publicDb.getInternalSession();
// @ts-expect-error Trusted reserved sessions are not public configuration.
void publicDb.getConfig().trustedReservedSession;

declare const account: import("../index.js").AccountHandle;
const enrolled: PackageDbConfig = { appId: "app", serverUrl: "https://core.example", account };
const runtimeEnrolled: RuntimeDbConfig = enrolled;
// @ts-expect-error Public contexts require an enrolled account handle.
const unauthenticated: PackageDbConfig = { appId: "app" };
// @ts-expect-error Credentials are enrolled outside contexts.
const localFirst: PackageDbConfig = { ...enrolled, secret: "secret" };
// @ts-expect-error External JWTs must be registered or logged in first.
const jwt: RuntimeDbConfig = { ...enrolled, jwtToken: "jwt" };
// @ts-expect-error Cookie claims cannot replace account admission.
const cookie: RuntimeDbConfig = { ...enrolled, cookieSession };
// @ts-expect-error Admin credentials belong to backend APIs.
const admin: PackageDbConfig = { ...enrolled, adminSecret: "admin" };
// @ts-expect-error Backend credentials belong to jazz-tools/backend.
const backend: PackageDbConfig = { ...enrolled, backendSecret: "backend" };
// @ts-expect-error Raw account IDs cannot substitute for opaque handles.
const forged: PackageDbConfig = { appId: "app", account: { id: "account" } };
// @ts-expect-error Registry authority is obtained only from the handle.
const forgedAuthority: PackageDbConfig = {
  ...enrolled,
  accountRegistryAuthority: "https://other.example",
};
void forgedAuthority;
void runtimeEnrolled;
void forged;

void packageInsertOptions;
void runtimeInsertOptions;
void unauthenticated;
void localFirst;
void jwt;
void cookie;
void admin;
void backend;
void (null as unknown as PackageCreateOptions);
void (null as unknown as RuntimeCreateOptions);
