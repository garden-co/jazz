/**
 * `createJazz`'s side of Rust's `jazz::tools::backend_admission`: the one
 * place who a socket is admitted as, and what reaches the core, is decided.
 * This answers the lookups Rust asks for and decides nothing itself.
 */
import { backendNative } from "./backend-native.js";

/** The host's lookups, each run only when the admission needs it. */
export interface AdmissionHost {
  readonly backendSecret: string;
  readonly appId: string;
  /** What the request's backend-client credential is trusted as. */
  backendClient(): Promise<"none" | "refused" | "trusted" | "scoped">;
  /** The endpoint URLs a device account's proof may name besides the app. */
  endpoints(): string[];
  /** Whether the app lets this verified device account in. */
  deviceAccount(endpoint: string | undefined): Promise<boolean>;
  /** The user `authenticate` names. */
  user(): Promise<string | null>;
  /** The named user's account; null `id` for a user with none. */
  account(): Promise<{ id: string | null; issuer: string; claims: unknown }>;
  /** A device account's proof that doesn't hold here was refused. */
  unverifiedDeviceProof(token: string): void;
}

type Step =
  | { step: "lookup"; lookup: "backendClient" | "user" | "account" }
  | { step: "lookup"; lookup: "deviceAccount"; endpoint: string | null }
  | { step: "refused"; refusal: "missing" | "unverified" }
  | { step: "refused"; refusal: "unverifiedDeviceProof"; token: string }
  | {
      step: "admitted";
      prelude: string;
      who: "backendClient" | "deviceAccount" | "user";
      access: "session" | "backend";
      renewal: boolean;
    };

export type Admission =
  | { refused: "missing" | "unverified" }
  | (Omit<Extract<Step, { step: "admitted" }>, "step" | "prelude"> & { prelude: Uint8Array });

export async function admitClient(prelude: Uint8Array, host: AdmissionHost): Promise<Admission> {
  const facts: Record<string, unknown> = {
    backendSecret: host.backendSecret,
    appId: host.appId,
    nowMs: Date.now(),
  };
  for (;;) {
    const step = JSON.parse(backendNative().admitPrelude(prelude, JSON.stringify(facts))) as Step;
    if (step.step === "refused") {
      if (step.refusal !== "unverifiedDeviceProof") return { refused: step.refusal };
      host.unverifiedDeviceProof(step.token);
      return { refused: "unverified" };
    }
    if (step.step === "admitted") {
      const { who, access, renewal } = step;
      return { prelude: new TextEncoder().encode(step.prelude), who, access, renewal };
    }
    switch (step.lookup) {
      case "backendClient":
        facts.client = await host.backendClient();
        facts.endpoints = host.endpoints();
        // Proofs are judged when the credential lookup (which may fetch an
        // issuer's keys) is done.
        facts.nowMs = Date.now();
        break;
      case "deviceAccount":
        facts.deviceAccepted = await host.deviceAccount(step.endpoint ?? undefined);
        break;
      case "user": {
        // Before `authenticate` runs: the core compares it with the user's
        // revocation mark, so a `revokeUser` on any instance after this
        // moment ends the session, and one before it refuses the session.
        const authenticatedAtMs = Date.now();
        facts.user = { userId: await host.user(), authenticatedAtMs };
        break;
      }
      case "account":
        facts.account = await host.account();
        break;
      default:
        throw new Error("createJazz: the engine asked for an admission lookup it doesn't know");
    }
  }
}
