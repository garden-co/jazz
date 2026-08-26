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

// @ts-expect-error Internal transport sessions are not a public package type.
import type { Session } from "../index.js";

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
publicSession.user satisfies string;
// @ts-expect-error Raw transport issuer is not exposed by PublicSession.
publicSession.issuer;
// @ts-expect-error Raw transport subject is not exposed by PublicSession.
publicSession.user_id;
userIdentity("https://issuer.example", "user") satisfies string;

declare const publicDb: import("../index.js").Db;
// @ts-expect-error Framework transport session access is package-private.
publicDb.getInternalSession();
// @ts-expect-error Trusted reserved sessions are not public configuration.
publicDb.getConfig().trustedReservedSession;

const unauthenticated: PackageDbConfig = { appId: "app" };
const localFirst: PackageDbConfig = { appId: "app", secret: "secret" };
const jwt: RuntimeDbConfig = { appId: "app", jwtToken: "jwt" };
const cookie: RuntimeDbConfig = {
  appId: "app",
  cookieSession,
};
const admin: PackageDbConfig = { appId: "app", adminSecret: "admin" };
const backend: PackageDbConfig = {
  appId: "app",
  backendSecret: "backend",
  cookieSession,
};
const optionalJwt: PackageDbConfig = {
  appId: "app",
  jwtToken: undefined as string | undefined,
};

// @ts-expect-error Local-first and JWT authentication are mutually exclusive.
const localFirstWithJwt: PackageDbConfig = { appId: "app", secret: "secret", jwtToken: "jwt" };
// @ts-expect-error Local-first and cookie authentication are mutually exclusive.
const localFirstWithCookie: PackageDbConfig = { appId: "app", secret: "secret", cookieSession };
// @ts-expect-error JWT and cookie authentication are mutually exclusive.
const jwtWithCookie: RuntimeDbConfig = { appId: "app", jwtToken: "jwt", cookieSession };

void packageInsertOptions;
void runtimeInsertOptions;
void unauthenticated;
void localFirst;
void jwt;
void cookie;
void admin;
void backend;
void optionalJwt;
void localFirstWithJwt;
void localFirstWithCookie;
void jwtWithCookie;
void (null as unknown as PackageCreateOptions);
void (null as unknown as RuntimeCreateOptions);
