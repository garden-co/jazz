import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import {
  acceptDeviceAccounts,
  createTestJazz,
  remote,
  type CreateJazzOptions,
  type Jazz,
} from "../backend/create-jazz.js";
import type { Session } from "../runtime/context.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import type { SchemaSourceInput } from "../schema-source.js";
import {
  deploy,
  startLocalJazzServer,
  type DeployOptions,
  type LocalJazzServerHandle,
  type StartLocalJazzServerOptions,
} from "../dev/dev-server.js";
import { resolveAppPermissions, type BackendPermissions, type DeployResult } from "../dev/catalogue.js";

/** What a client needs to sync as a signed-in user, spread into its config. */
export interface TestUserConfig {
  appId: string;
  /** This user's own origin's `/jazz` endpoint. */
  serverUrl: string;
  /** The session the app's backend vouches for on this user's sockets. */
  cookieSession: Session;
}

export interface TestJazz {
  readonly appId: string;
  /** The app's `/jazz` endpoint, for guests (local-first and anonymous clients). */
  readonly url: string;
  /** The core behind it: for deploys, admin calls and the backend's own clients. */
  readonly core: LocalJazzServerHandle;
  /** The core's admin secret. */
  readonly adminSecret: string;
  /** The core's backend secret, for the backend's own clients. */
  readonly backendSecret: string;
  /** The app's Jazz, fronting the core (replaced by {@link TestJazz.redeploy}). */
  readonly jazz: Jazz;
  /**
   * Sync as `userId`, signed in to the app with `claims`. Each user reaches
   * the app on their own origin, the way their own browser's cookie would
   * name them, and the app's `authenticate` reads who they are from it.
   */
  as(userId: string, claims?: Record<string, unknown>): Promise<TestUserConfig>;
  /**
   * An opaque bearer token the app's `authenticate` reads as `userId`, for a
   * client that sends `Authorization: Bearer` (a native app) to {@link url}.
   */
  bearer(userId: string, claims?: Record<string, unknown>): string;
  /**
   * Sign requests that reach the app with Host `host` in as the user whose
   * origin is `userUrl` (from {@link as}): for a proxy in front of their
   * origin, such as a transport control, which forwards their browser's
   * requests unchanged. Returns false (and signs nothing in) when `userUrl`
   * is not one of this app's user origins.
   */
  proxyOrigin(userUrl: string, host: string): boolean;
  /**
   * Sign the user whose origin is `userUrl` (from {@link as}) out, as the
   * app's logout clears their cookie: the app's `authenticate` names no one
   * there from now on. Returns false when `userUrl` is not one of this app's
   * user origins.
   */
  signOut(userUrl: string): boolean;
  /**
   * A new origin of the app where no one is signed in yet, as a visitor's own
   * browser: its `/jazz` endpoint. Sign them up there with {@link signUp}.
   */
  visitor(): Promise<string>;
  /**
   * Sign `userId` in on the origin `originUrl` (from {@link visitor}), as the
   * app's own sign-up sets their cookie: the app's `authenticate` names them
   * there from now on. No Jazz account is made for them: a guest who signs
   * up there keeps theirs with `linkDeviceAccount()`. Returns false when
   * `originUrl` is not one of this app's origins.
   */
  signUp(originUrl: string, userId: string, claims?: Record<string, unknown>): boolean;
  /**
   * Deploy a new version of the app, as a real redeploy does: publish it to
   * the core (with its migration), then restart the app's Jazz on it. Every
   * origin keeps its port, so clients reconnect where they were.
   */
  redeploy(
    next: Pick<DeployOptions, "migration" | "migrations" | "schemas"> & {
      app: SchemaSourceInput;
      permissions: BackendPermissions;
    },
  ): Promise<DeployResult>;
  /** Stop the app's Jazz, its origins and (if this started it) the core. */
  stop(): Promise<void>;
}

export interface StartTestJazzOptions {
  app: SchemaSourceInput;
  /** The app's `jazz.permissions.json`, or `definePermissions`' map for an app on `{ migrations: "none" }`. */
  permissions: BackendPermissions;
  /** A running core to front; by default one is started in memory. */
  core?: LocalJazzServerHandle;
  /** Options for the core started when `core` is not given. */
  coreOptions?: Omit<StartLocalJazzServerOptions, "schema" | "permissions">;
  /**
   * Port for the guests' origin (default: any free port). A test that
   * redeploys the app passes the old origin's port to a new `startTestJazz`,
   * so clients keep their origin, as across a real redeploy.
   */
  port?: number;
  /**
   * Whether the app lets local-first guests sync, as an app's `authenticate`
   * does by returning `acceptDeviceAccounts(request)` (default true).
   */
  guests?: boolean;
  /** More `createJazz` options (not `app`, `permissions`, `store` or `authenticate`). */
  jazz?: Omit<CreateJazzOptions, "app" | "permissions" | "store" | "authenticate">;
}

/**
 * A core behind the app's own Jazz (`createJazz` with `remote()`), as an app
 * runs it: a core admits only its app's backend, so test clients sync
 * through it. Guests use {@link TestJazz.url}; signed-in users use
 * {@link TestJazz.as}.
 */
export async function startTestJazz(options: StartTestJazzOptions): Promise<TestJazz> {
  const ownsCore = !options.core;
  const core =
    options.core ?? (await startLocalJazzServer({ inMemory: true, ...options.coreOptions }));
  // Which user each origin (by its Host) is signed in as.
  const users = new Map<
    string,
    { userId: string; claims?: Record<string, unknown>; expiresAt?: number }
  >();
  // Who each bearer from `bearer()` is.
  const bearers = new Map<
    string,
    { userId: string; claims?: Record<string, unknown>; expiresAt?: number }
  >();
  // Origins from `visitor()`, where no one signed in yet.
  const visitors = new Set<string>();
  const userFor = (request: Request) => {
    const authorization = request.headers.get("authorization");
    const user = authorization?.startsWith("Bearer ")
      ? bearers.get(authorization.slice(7))
      : users.get(new URL(request.url).host);
    // A test user's sign-in holds for an hour unless the test says otherwise.
    if (user) return { ...user, expiresAt: user.expiresAt ?? Math.floor(Date.now() / 1000) + 3600 };
    return options.guests === false ? null : acceptDeviceAccounts(request);
  };
  const servers: Server[] = [];
  // Every connection, including upgraded sync sockets, which `server.close`
  // would otherwise wait on: a redeploy or stop drops them, as a real one does.
  const sockets = new Set<Socket>();
  const boot = (app: SchemaSourceInput, permissions: BackendPermissions) =>
    createTestJazz({
      ...options.jazz,
      app: app as CreateJazzOptions["app"],
      permissions,
      store: remote({ url: core.url, appId: core.appId, secret: core.adminSecret }),
      authenticate: userFor,
    }).ready();
  let jazz: Jazz;
  try {
    jazz = await boot(options.app, options.permissions);
  } catch (error) {
    if (ownsCore) await core.stop();
    throw error;
  }

  async function serve(port = 0): Promise<string> {
    const server = createServer((req, res) =>
      jazz.nodeHandler(req, res, () => {
        res.statusCode = 404;
        res.end();
      }),
    );
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    });
    await jazz.attach(server);
    await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
    servers.push(server);
    return `127.0.0.1:${(server.address() as AddressInfo).port}`;
  }
  async function closeOrigins(): Promise<void> {
    for (const socket of sockets) socket.destroy();
    await jazz.close();
    await Promise.all(
      servers.map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
    );
  }

  const guestHost = await serve(options.port);
  let stopped: Promise<void> | undefined;
  const handle: TestJazz = {
    appId: core.appId,
    url: `http://${guestHost}${jazz.basePath}`,
    core,
    adminSecret: core.adminSecret,
    backendSecret: core.backendSecret,
    get jazz() {
      return jazz;
    },
    bearer(userId, claims) {
      const token = `test-bearer-${randomUUID()}`;
      bearers.set(token, { userId, claims });
      return token;
    },
    proxyOrigin(userUrl, host) {
      const user = users.get(new URL(userUrl).host);
      if (!user) return false;
      users.set(host, user);
      return true;
    },
    signOut(userUrl) {
      return users.delete(new URL(userUrl).host);
    },
    async visitor() {
      const host = await serve();
      visitors.add(host);
      return `http://${host}${jazz.basePath}`;
    },
    signUp(originUrl, userId, claims) {
      const host = new URL(originUrl).host;
      if (!visitors.has(host) && !users.has(host)) return false;
      users.set(host, { userId, claims });
      return true;
    },
    async as(userId, claims) {
      const host = await serve();
      users.set(host, { userId, claims });
      // Signed in as their browser would be: the sign-in gives them their account.
      const signIn = await jazz.handle(
        new Request(`http://${host}${jazz.basePath}/apps/${core.appId}/accounts/backend-sign-in`, {
          method: "POST",
        }),
      );
      if (!signIn.ok)
        throw new Error(`startTestJazz: ${userId} could not sign in (${signIn.status})`);
      const { account, identity } = (await signIn.json()) as {
        account: string;
        identity: { issuer: string };
      };
      return {
        appId: core.appId,
        serverUrl: `http://${host}${jazz.basePath}`,
        cookieSession: {
          account_id: account,
          issuer: identity.issuer,
          user_id: userId,
          claims: claims ?? {},
          authMode: "external",
        },
      };
    },
    async redeploy({ app, permissions, ...history }) {
      const deployed = await deploy({
        ...history,
        serverUrl: core.url,
        appId: core.appId,
        adminSecret: core.adminSecret,
        schema: app,
        permissions: (await resolveAppPermissions(app, permissions)).permissions,
      });
      const ports = servers.map((server) => (server.address() as AddressInfo).port);
      await closeOrigins();
      servers.length = 0;
      jazz = await boot(app, permissions);
      for (const port of ports) await serve(port);
      return deployed;
    },
    stop() {
      return (stopped ??= (async () => {
        await closeOrigins();
        if (ownsCore) await core.stop();
      })());
    },
  };
  return handle;
}
