import { betterAuth, type BetterAuthOptions, type DBAdapter } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { sveltekitCookies } from "better-auth/svelte-kit";
import type { RequestEvent } from "@sveltejs/kit";

export type TestAuthAdapter = DBAdapter<BetterAuthOptions>;

type SessionUpdateGate = {
  token: string;
  entered: () => void;
  released: Promise<void>;
  error?: Error;
};

const memoryDb: MemoryDB = {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
};

let requestEvent: RequestEvent | undefined;
let sessionUpdateGate: SessionUpdateGate | undefined;

const database = (options: BetterAuthOptions): TestAuthAdapter => {
  const adapter = memoryAdapter(memoryDb)(options);
  const update = adapter.update;

  adapter.update = async (input) => {
    const token = input.where.find((condition) => condition.field === "token")?.value;
    const gate = sessionUpdateGate;
    if (gate && input.model === "session" && token === gate.token) {
      sessionUpdateGate = undefined;
      gate.entered();
      if (gate.error) throw gate.error;
      await gate.released;
    }
    return update(input);
  };

  return adapter;
};

export const auth = betterAuth({
  baseURL: "http://localhost:5173",
  secret: "test-secret-do-not-use-in-production-0123456789",
  database,
  trustedOrigins: ["http://localhost:5173"],
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  session: {
    expiresIn: 60 * 60,
    updateAge: 30 * 60,
  },
  plugins: [
    // sveltekitCookies must be the last plugin.
    sveltekitCookies(() => requestEvent as RequestEvent),
  ],
});

export function setTestRequestEvent(event: RequestEvent): void {
  requestEvent = event;
}

export function clearTestRequestEvent(): void {
  requestEvent = undefined;
}

export function pauseSessionUpdate(token: string): {
  reached: Promise<void>;
  release: () => void;
} {
  let markReached!: () => void;
  let releaseUpdate!: () => void;
  const reached = new Promise<void>((resolve) => {
    markReached = resolve;
  });
  const released = new Promise<void>((resolve) => {
    releaseUpdate = resolve;
  });

  sessionUpdateGate = {
    token,
    entered: markReached,
    released,
  };

  return { reached, release: releaseUpdate };
}

export function failSessionUpdate(token: string): { reached: Promise<void> } {
  let markReached!: () => void;
  const reached = new Promise<void>((resolve) => {
    markReached = resolve;
  });

  sessionUpdateGate = {
    token,
    entered: markReached,
    released: Promise.resolve(),
    error: new Error("database unavailable during session renewal"),
  };

  return { reached };
}

export function clearSessionUpdateGate(): void {
  sessionUpdateGate = undefined;
}

async function getTestAdapter(): Promise<TestAuthAdapter> {
  const context = await auth.$context;
  return context.adapter;
}

export async function makeSessionEligibleForRefresh(token: string): Promise<void> {
  const adapter = await getTestAdapter();
  const updated = await adapter.update({
    model: "session",
    where: [{ field: "token", operator: "eq", value: token }],
    update: {
      // Five minutes leaves enough time for the deterministic request while
      // remaining inside the configured rolling-renewal window.
      expiresAt: new Date(Date.now() + 5 * 60 * 1000),
    },
  });
  if (!updated) throw new Error("Test session was not found while making it refreshable");
}

export async function deleteSessionViaAdapter(token: string): Promise<void> {
  const adapter = await getTestAdapter();
  await adapter.delete({
    model: "session",
    where: [{ field: "token", operator: "eq", value: token }],
  });
}
export async function getSessionCookieNames(): Promise<{
  sessionToken: string;
  sessionData: string;
}> {
  const context = await auth.$context;
  return {
    sessionToken: context.authCookies.sessionToken.name,
    sessionData: context.authCookies.sessionData.name,
  };
}
