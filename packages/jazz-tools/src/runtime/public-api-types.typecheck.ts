import type {
  DbConfig as PackageDbConfig,
  InsertOptions as PackageInsertOptions,
} from "../index.js";
import type {
  DbConfig as RuntimeDbConfig,
  InsertOptions as RuntimeInsertOptions,
} from "./index.js";

// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as PackageCreateOptions } from "../index.js";
// @ts-expect-error CreateOptions was renamed to InsertOptions.
import type { CreateOptions as RuntimeCreateOptions } from "./index.js";

const packageInsertOptions: PackageInsertOptions = { id: "row-1", updatedAt: 1 };
const runtimeInsertOptions: RuntimeInsertOptions = { id: "row-1", updatedAt: 1 };

const unauthenticated: PackageDbConfig = { appId: "app" };
const localFirst: PackageDbConfig = { appId: "app", secret: "secret" };
const jwt: RuntimeDbConfig = { appId: "app", jwtToken: "jwt" };
const cookie: RuntimeDbConfig = {
  appId: "app",
  cookieSession: { user_id: "user", claims: {}, authMode: "external" },
};
const admin: PackageDbConfig = { appId: "app", adminSecret: "admin" };
const backend: PackageDbConfig = {
  appId: "app",
  backendSecret: "backend",
  cookieSession: { user_id: "user", claims: {}, authMode: "external" },
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
  cookieSession: { user_id: "user", claims: {}, authMode: "external" },
};
// @ts-expect-error JWT and cookie authentication are mutually exclusive.
const jwtWithCookie: RuntimeDbConfig = {
  appId: "app",
  jwtToken: "jwt",
  cookieSession: { user_id: "user", claims: {}, authMode: "external" },
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
