import {
  AccountError,
  AccountRegistry,
  type AccountCommand,
  type AccountCommandResult,
  type Assignment,
  type Principal,
  validatePrincipal,
} from "../account_registry.js";
import * as codec from "./codec.js";
import { concatBytes, equalBytes, utf8 } from "../../support/bytes.js";
import { uuidToString, uuidV4 } from "../../support/hash.js";
import { RecordStore, ScanBounds, type OrderedKvStorage } from "../../_extern/types-protocol.js";

const CF = "default";
const PREFIX = utf8("account-command:v1:");

export interface LoginOrRegisterResult {
  assignment: Assignment;
  created: boolean;
}

export class RegistryUnavailable extends Error {
  constructor(readonly detail: string) {
    super(`account registry unavailable: ${detail}`);
  }
}
const unavailable = (e: unknown) =>
  new RegistryUnavailable(e instanceof Error ? e.message : String(e));

function commandKey(revision: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(revision));
  return concatBytes(PREFIX, b);
}

/** Durable ordered command journal for one application registry. */
export class StoredAccountRegistry {
  private constructor(
    private storage: OrderedKvStorage,
    private state: AccountRegistry,
    private revision: number,
    private poisoned: boolean,
  ) {}

  static async open(storage: OrderedKvStorage): Promise<StoredAccountRegistry> {
    const state = new AccountRegistry();
    let revision = 0;
    try {
      const records = new RecordStore(storage, CF, codec.descriptor());
      const scan = await records.scan(ScanBounds.Prefix(utf8("account-command:")));
      for (let batch = await scan.nextBatch(); batch; batch = await scan.nextBatch()) {
        for (const [key, bytes] of batch) {
          if (!equalBytes(key, commandKey(revision)))
            throw new Error("invalid account journal sequence");
          state.apply(codec.decode(bytes));
          revision += 1;
        }
      }
      // Never serve replayed state before an earlier append's durability barrier.
      await storage.flushWriteBoundary();
    } catch (e) {
      throw unavailable(e);
    }
    return new StoredAccountRegistry(storage, state, revision, false);
  }

  async login(principal: Principal): Promise<Assignment> {
    this.ensureAvailable();
    this.poisoned = true;
    let advanced: unknown;
    try {
      advanced = await new RecordStore(this.storage, CF, codec.descriptor()).get(
        commandKey(this.revision),
      );
    } catch (e) {
      throw unavailable(e);
    }
    if (advanced !== undefined) throw unavailable("account authority advanced elsewhere");
    this.poisoned = false;
    return { ...this.state.login(principal) };
  }

  async loginOrRegister(principal: Principal): Promise<LoginOrRegisterResult> {
    validatePrincipal(principal, false);
    try {
      return { assignment: await this.login(principal), created: false };
    } catch (e) {
      if (!(e instanceof AccountError && e.kind === "NotAssigned")) throw e;
    }
    const result = await this.execute({
      kind: "Register",
      principal,
      account: uuidToString(uuidV4()),
    });
    if (result.kind !== "Assignment") throw new Error("registration returns an assignment");
    return { assignment: result.assignment, created: true };
  }

  async execute(command: AccountCommand): Promise<AccountCommandResult> {
    this.ensureAvailable();
    const candidate = this.state.clone();
    const result = candidate.apply(command);
    let bytes: Uint8Array;
    try {
      bytes = codec.encode(command);
    } catch (e) {
      throw unavailable(e);
    }
    this.poisoned = true;
    let existing: unknown;
    try {
      existing = await new RecordStore(this.storage, CF, codec.descriptor()).putIfAbsent(
        commandKey(this.revision),
        bytes,
      );
    } catch (e) {
      throw unavailable(e);
    }
    if (existing !== undefined) throw unavailable("concurrent account authority detected");
    try {
      await this.storage.flushWriteBoundary();
    } catch (e) {
      throw unavailable(e);
    }
    this.state = candidate;
    this.revision += 1;
    this.poisoned = false;
    return result;
  }

  async close(): Promise<void> {
    try {
      await this.storage.close();
    } catch (e) {
      throw unavailable(e);
    }
  }

  private ensureAvailable() {
    if (this.poisoned) throw unavailable("authority requires recovery");
  }
}
