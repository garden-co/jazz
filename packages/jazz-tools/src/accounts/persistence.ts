import { setAccountSelectionBarrier } from "./selection-durability.js";
import {
  createAccountManagerWithRuntime,
  type BackendAccountHost,
  type JWTAuth,
  type FounderOwnership,
} from "./enrollment.js";
import type { AccountHandle, AccountManager } from "./state.js";
import { localFirstFactory } from "./local-first.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";

/** Platform storage: use browser storage or an OS-protected store on native hosts. */
export interface AccountStore {
  read(): Promise<string | null>;
  /** Atomically read/transform/write across every manager sharing this store.
   * The callback is synchronous and may be retried by a transactional host.
   * Resolve only after the replacement is durable; leave the old value on failure.
   */
  update(transform: (current: string | null) => string): Promise<void>;
}

// Versioned ownership prevents old writers from silently erasing first-device
// claims while changing selection. Legacy account origin is not device proof.
interface FounderClaim {
  root: string;
  scope: string;
  deviceId: string;
  epochId: string | null;
  closed: boolean;
}
interface StoredAccounts {
  format: "jazz-account-selection-v3";
  roots: string[];
  selected: number | null;
  generatedHere: string[];
  founderEligibleRoots: string[];
  founders: FounderClaim[];
}
function decode(value: string | null): StoredAccounts {
  if (value === null)
    return {
      format: "jazz-account-selection-v3",
      roots: [],
      selected: null,
      generatedHere: [],
      founderEligibleRoots: [],
      founders: [],
    };
  const parsed = JSON.parse(value) as Omit<StoredAccounts, "format"> & { format: string };
  if (
    (parsed?.format !== "jazz-account-selection-v1" &&
      parsed?.format !== "jazz-account-selection-v2" &&
      parsed?.format !== "jazz-account-selection-v3") ||
    !Array.isArray(parsed.roots) ||
    parsed.roots.some((root) => typeof root !== "string") ||
    (parsed.selected !== null &&
      (!Number.isInteger(parsed.selected) ||
        parsed.selected < 0 ||
        parsed.selected >= parsed.roots.length))
  ) {
    throw new Error("Invalid persisted account selection");
  }
  for (const root of parsed.roots) parseAuthSecret(root);
  // A v1 inventory never proves local creation, even if an unknown extension
  // claims otherwise. Migration retains its selection and every secret root.
  const generatedHere = parsed.format === "jazz-account-selection-v1" ? [] : parsed.generatedHere;
  if (
    !Array.isArray(generatedHere) ||
    generatedHere.some((root) => typeof root !== "string" || !parsed.roots.includes(root)) ||
    new Set(generatedHere).size !== generatedHere.length
  )
    throw new Error("Invalid persisted account creation provenance");
  // Presence, not truthiness or list length, distinguishes the candidate writer
  // from ordinary v2. Even its empty ownership list proves that format lineage.
  const candidate =
    parsed.format === "jazz-account-selection-v2" && Object.hasOwn(parsed, "founders");
  const founderEligibleRoots =
    parsed.format === "jazz-account-selection-v3"
      ? parsed.founderEligibleRoots
      : candidate
        ? generatedHere
        : [];
  if (
    !Array.isArray(founderEligibleRoots) ||
    founderEligibleRoots.some(
      (root) => typeof root !== "string" || !generatedHere.includes(root),
    ) ||
    new Set(founderEligibleRoots).size !== founderEligibleRoots.length
  )
    throw new Error("Invalid persisted founder eligibility");
  const founders =
    parsed.format === "jazz-account-selection-v3" || candidate ? parsed.founders : [];
  if (!Array.isArray(founders)) throw new Error("Invalid persisted founder ownership");
  const scopes = new Set<string>();
  for (const claim of founders) {
    if (
      !claim ||
      typeof claim.root !== "string" ||
      !parsed.roots.includes(claim.root) ||
      typeof claim.scope !== "string" ||
      !claim.scope ||
      typeof claim.deviceId !== "string" ||
      !claim.deviceId ||
      (claim.epochId !== null && (typeof claim.epochId !== "string" || !claim.epochId)) ||
      typeof claim.closed !== "boolean" ||
      (!claim.closed && !founderEligibleRoots.includes(claim.root))
    )
      throw new Error("Invalid persisted founder ownership");
    const key = JSON.stringify([claim.root, claim.scope]);
    if (scopes.has(key)) throw new Error("Duplicate persisted founder ownership");
    scopes.add(key);
  }
  return {
    format: "jazz-account-selection-v3",
    roots: [...parsed.roots],
    selected: parsed.selected,
    generatedHere: [...generatedHere],
    founderEligibleRoots: [...founderEligibleRoots],
    founders,
  };
}

function founderOwnership(
  store: AccountStore,
  root: string,
  assertValid: () => void,
  retained: Promise<void>,
): FounderOwnership {
  const update = async <T>(
    scope: string,
    deviceId: string,
    change: (state: StoredAccounts, claim: FounderClaim | undefined) => T,
  ): Promise<T> => {
    assertValid();
    await retained;
    assertValid();
    if (!scope || !deviceId) throw new Error("Invalid founder ownership request");
    let result: { value: T } | undefined;
    await store.update((current) => {
      // Transactional stores can defer or retry this synchronous callback.
      assertValid();
      const state = decode(current);
      if (!state.roots.includes(root)) throw new Error("Founder account root is not retained");
      const claim = state.founders.find((entry) => entry.root === root && entry.scope === scope);
      result = { value: change(state, claim) };
      return JSON.stringify(state);
    });
    assertValid();
    if (!result) throw new Error("Account store did not perform its atomic update");
    return result.value;
  };
  return {
    reserve: (scope, deviceId, retainedEpochId) =>
      update(scope, deviceId, (state, claim) => {
        if (retainedEpochId !== undefined && !retainedEpochId)
          throw new Error("Invalid retained founder epoch");
        if (claim) {
          if (claim.deviceId !== deviceId || (claim.closed && claim.epochId === null))
            return undefined;
          // The lifecycle owns validating this epoch against its exact journal.
          // Reporting a bound epoch never grants permission to replace it.
          return { epochId: claim.epochId };
        }
        const eligible = state.founderEligibleRoots.includes(root);
        if (!eligible && retainedEpochId === undefined) return undefined;
        state.founders.push({
          root,
          scope,
          deviceId,
          epochId: retainedEpochId ?? null,
          closed: !eligible,
        });
        return { epochId: retainedEpochId ?? null };
      }),
    bind: (scope, deviceId, epochId) =>
      update(scope, deviceId, (_state, claim) => {
        if (
          !epochId ||
          !claim ||
          claim.deviceId !== deviceId ||
          (claim.epochId !== null && claim.epochId !== epochId) ||
          (claim.closed && claim.epochId === null)
        )
          throw new Error("Founder proposal does not match its account ownership");
        claim.epochId = epochId;
      }),
    close: (scope, deviceId, retainedEpochId) =>
      update(scope, deviceId, (state, claim) => {
        if (!claim) {
          state.founders.push({ root, scope, deviceId, epochId: null, closed: true });
          return true;
        }
        // Closing without a bound offline proposal forbids future automatic
        // founders, not Global-exclusive arbitration between online devices.
        if (claim.closed && claim.epochId === null) return true;
        if (claim.deviceId !== deviceId) return false;
        if (claim.epochId !== null && claim.epochId !== retainedEpochId) return false;
        claim.closed = true;
        return claim.epochId === null && retainedEpochId === undefined;
      }),
  };
}

const automaticInitializers = new WeakMap<AccountManager<JWTAuth>, () => Promise<AccountHandle>>();

/** @internal Select and adopt one durable root for automatic first startup. */
export async function ensureAutomaticLocalFirst(
  accounts: AccountManager<JWTAuth>,
): Promise<AccountHandle> {
  const selected = accounts.getLoggedIn();
  if (selected) return selected;
  return automaticInitializers.get(accounts)?.() ?? accounts.createLocalFirst();
}

/** @internal Hosts load native crypto first; all selection semantics stay shared. */
export async function prepareAccountManager(options: {
  appId: string;
  registry: string;
  store: AccountStore;
  mintToken(secret: string, audience: string): string;
  generateSecret?(): string;
  fetch?: typeof fetch;
  backend?: BackendAccountHost;
}) {
  const stored = decode(await options.store.read());
  let writes = Promise.resolve();
  let adoptingSecret: string | undefined;
  const save = () => {
    const roots = [...stored.roots];
    const generatedHere = [...stored.generatedHere];
    const founderEligibleRoots = [...stored.founderEligibleRoots];
    const selected = stored.selected === null ? null : stored.roots[stored.selected]!;
    // Serialize snapshots even for an asynchronous native secure store. A
    // rejected save does not prevent a later explicit selection from retrying.
    writes = writes
      .catch(() => {})
      .then(() =>
        options.store.update((current) => {
          const latest = decode(current);
          // Independent managers may have discovered roots since we loaded. Never
          // replace their key inventory with this manager's older snapshot.
          for (const root of roots) {
            if (latest.roots.includes(root)) continue;
            latest.roots.push(root);
            if (founderEligibleRoots.includes(root)) latest.founderEligibleRoots.push(root);
          }
          latest.generatedHere = [...new Set([...latest.generatedHere, ...generatedHere])];
          latest.selected = selected === null ? null : latest.roots.indexOf(selected);
          return JSON.stringify(latest);
        }),
      );
    void writes.catch(() => {});
    return writes;
  };
  const manager = createAccountManagerWithRuntime({
    registry: options.registry,
    fetch: options.fetch,
    backend: options.backend,
    restoredLocalFirstSecret: stored.selected === null ? undefined : stored.roots[stored.selected],
    localFirst: localFirstFactory({
      appId: options.appId,
      mintToken: options.mintToken,
      generateSecret: options.generateSecret,
      isSecretRetained: async (secret) => decode(await options.store.read()).roots.includes(secret),
      isGeneratedHere: async (secret) =>
        decode(await options.store.read()).generatedHere.includes(secret),
      founderOwnership: (secret, assertValid, retained) =>
        founderOwnership(options.store, secret, assertValid, retained),
      retainSecret(secret, generatedHere) {
        if (generatedHere) {
          stored.generatedHere = [...new Set([...stored.generatedHere, secret])];
          if (!stored.roots.includes(secret)) stored.founderEligibleRoots.push(secret);
        }
        if (adoptingSecret === secret) {
          // Suppress only the one retention callback caused by adoption. Any
          // re-entrant explicit selection must retain and persist normally.
          adoptingSecret = undefined;
          let index = stored.roots.indexOf(secret);
          if (index < 0) index = stored.roots.push(secret) - 1;
          stored.selected = index;
          return;
        }
        let index = stored.roots.indexOf(secret);
        if (index < 0) index = stored.roots.push(secret) - 1;
        stored.selected = index;
        return save();
      },
    }),
  });
  automaticInitializers.set(manager, async () => {
    let changed = false;
    let previous = manager.getSnapshot();
    const unsubscribe = manager.subscribe(() => {
      const next = manager.getSnapshot();
      // An error-only publish (e.g. reportPersistenceError) leaves the selection
      // intact; every other publish (select, logout, operation) supersedes.
      const errorOnly =
        next.error !== undefined &&
        next.account === previous.account &&
        next.pending === previous.pending;
      previous = next;
      if (!errorOnly) changed = true;
    });
    try {
      // Generate outside the transform because transactional hosts may retry it.
      const candidate = options.generateSecret?.() ?? generateAuthSecret();
      parseAuthSecret(candidate);
      let winner: string | undefined;
      writes = writes
        .catch(() => {})
        .then(() =>
          options.store.update((current) => {
            const latest = decode(current);
            const selected = latest.selected === null ? undefined : latest.roots[latest.selected];
            if (selected === undefined) {
              const index = latest.roots.indexOf(candidate);
              latest.selected = index >= 0 ? index : latest.roots.push(candidate) - 1;
              // Only a newly retained local candidate proves generation here.
              // Reusing an imported or legacy root must not grant provenance.
              if (index < 0) {
                latest.generatedHere.push(candidate);
                latest.founderEligibleRoots.push(candidate);
              }
              winner = candidate;
            } else {
              winner = selected;
            }
            return JSON.stringify(latest);
          }),
        );
      await writes;
      if (changed) {
        const current = manager.getLoggedIn();
        if (current) return current;
        stored.selected = null;
        await save();
        throw new Error("Automatic local-first startup was superseded");
      }
      if (winner === undefined)
        throw new Error("Automatic local-first selection was not committed");
      // Adoption only hydrates the manager; suppress its one retention callback.
      adoptingSecret = winner;
      try {
        manager.restoreLocalFirst(winner);
      } finally {
        if (adoptingSecret === winner) adoptingSecret = undefined;
      }
      const current = manager.getLoggedIn();
      if (current) return current;
      stored.selected = null;
      await save();
      throw new Error("Automatic local-first startup was superseded");
    } finally {
      unsubscribe();
    }
  });
  let selection = manager.getLoggedIn();
  manager.subscribe(() => {
    const next = manager.getLoggedIn();
    if (next === selection) return;
    selection = next;
    // Creating/restoring a local handle already queued its root and selection.
    // External credentials are owned by the provider and never serialized here.
    if (next?.identity.issuer === "urn:jazz:local-first") return;
    stored.selected = null;
    void save().catch((error) => manager.reportPersistenceError(error));
  });
  setAccountSelectionBarrier(manager, (retry) => (retry ? writes.catch(() => save()) : writes));
  await writes;
  return manager;
}
