import type {
  DbConfig as PackageDbConfig,
  InsertOptions as PackageInsertOptions,
  PublicSession,
  Session,
} from "../index.js";
import type {
  DbConfig as RuntimeDbConfig,
  InsertOptions as RuntimeInsertOptions,
} from "./index.js";
import { userIdentity } from "../index.js";

// @ts-expect-error sessionAuthor was replaced by userIdentity without a compatibility alias.
import { sessionAuthor } from "../index.js";

// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as PackageCreateOptions } from "../index.js";
// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as RuntimeCreateOptions } from "./index.js";

const packageInsertOptions: PackageInsertOptions = { id: "row-1", updatedAt: 1 };
const runtimeInsertOptions: RuntimeInsertOptions = { id: "row-1", updatedAt: 1 };
const session: Session = {
  issuer: "https://issuer.example",
  user_id: "user",
  claims: {},
  authMode: "external",
};
declare const publicSession: PublicSession;
publicSession.user satisfies string;
userIdentity("https://issuer.example", "user") satisfies string;

const unauthenticated: PackageDbConfig = { appId: "app" };
const localFirst: PackageDbConfig = { appId: "app", secret: "secret" };
const jwt: RuntimeDbConfig = { appId: "app", jwtToken: "jwt" };
const cookie: RuntimeDbConfig = {
  appId: "app",
  cookieSession: session,
};
const admin: PackageDbConfig = { appId: "app", adminSecret: "admin" };
const backend: PackageDbConfig = {
  appId: "app",
  backendSecret: "backend",
  cookieSession: session,
};
const optionalJwt: PackageDbConfig = {
  appId: "app",
  jwtToken: undefined as string | undefined,
};

// @ts-expect-error Local-first and JWT authentication are mutually exclusive.
const localFirstWithJwt: PackageDbConfig = { appId: "app", secret: "secret", jwtToken: "jwt" };
// @ts-expect-error Local-first and cookie authentication are mutually exclusive.
const localFirstWithCookie: PackageDbConfig = {
  appId: "app",
  secret: "secret",
  cookieSession: session,
};
// @ts-expect-error JWT and cookie authentication are mutually exclusive.
const jwtWithCookie: RuntimeDbConfig = {
  appId: "app",
  jwtToken: "jwt",
  cookieSession: session,
};

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
