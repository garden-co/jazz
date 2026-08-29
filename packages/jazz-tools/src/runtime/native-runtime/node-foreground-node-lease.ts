/**
 * Node-only durable foreground TxId lease pool.
 *
 * There deliberately is no process-wide lock. A node's `active/<uuid>` file
 * is its exclusive O_EXCL claim, while `reusable/<uuid>` is a separately
 * durable high-water receipt. An abandoned active claim is never removed or
 * inferred safe: it permanently quarantines that UUID.
 *
 * This module is dynamically imported only from a Node runtime source. Keep
 * every `node:` import here so browser and React Native bundles never resolve
 * filesystem code.
 */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { mkdir, open, readFile, readdir, rename, unlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import type { ForegroundNodeLease } from "../runtime-source.js";

const FORMAT = "jazz-node-foreground-node-leases-v2";
const ACTIVE_DIRECTORY = "active";
const REUSABLE_DIRECTORY = "reusable";
const RETIRED_DIRECTORY = "retired";
const NODE_RE = /^[0-9a-f]{32}$/;
const MAX_TX_TIME = (1n << 64n) - 1n;

type ReusableReceipt = {
  format: typeof FORMAT;
  node: string;
  confirmedTxTime: string;
};
type ActiveClaim = { format: typeof FORMAT; node: string; token: string };

export type NodeForegroundNodeLeaseOptions = {
  appId: string;
  env: string;
  /** Stable non-secret auth namespace; never stores claims or credentials. */
  authScope: string;
};

/**
 * Acquire an exclusive NodeUuid for one live foreground runtime.
 *
 * A claim is created before its receipt is read. Thus two processes cannot
 * both consume a returned UUID, and a crash at any point leaves an active
 * claim which future processes skip forever. This intentionally trades a rare
 * UUID leak for no possibility of TxId reuse.
 */
export async function acquireNodeForegroundNodeLease(
  options: NodeForegroundNodeLeaseOptions,
): Promise<ForegroundNodeLease> {
  const directory = leaseDirectory(options);
  await ensureDirectories(directory);

  for (;;) {
    // Directory iteration is only a reuse optimisation. The O_EXCL claim is
    // the arbitration primitive.
    const candidates = await reusableNodes(directory);
    const node = candidates.shift() ?? randomBytes(16).toString("hex");
    const claim = await tryAcquireActiveClaim(directory, node);
    if (!claim) continue;
    try {
      if (await exists(retiredPath(directory, node))) {
        await leaveClaimQuarantined(claim);
        continue;
      }
      const receipt = await readReusableReceipt(directory, node);
      return new NodeForegroundNodeLease(
        directory,
        node,
        claim,
        receipt ? BigInt(receipt.confirmedTxTime) : 0n,
      );
    } catch (error) {
      // Do not remove a claim after any uncertain read/validation failure.
      await leaveClaimQuarantined(claim);
      throw error;
    }
  }
}

/** @internal Test-only locator for white-box durability receipts. */
export function nodeForegroundNodeLeaseDirectoryForTest(
  options: NodeForegroundNodeLeaseOptions,
): string {
  return leaseDirectory(options);
}

class NodeForegroundNodeLease implements ForegroundNodeLease {
  readonly node: Uint8Array;
  private finished = false;

  constructor(
    private readonly directory: string,
    private readonly nodeHex: string,
    private readonly claim: ActiveClaim,
    readonly confirmedTxTime: bigint,
  ) {
    this.node = Buffer.from(nodeHex, "hex");
  }

  async returnWithHighWater(runtimeHighWater: bigint): Promise<void> {
    if (this.finished || !isTxTime(runtimeHighWater)) {
      throw new Error("Invalid Node foreground lease handoff");
    }
    try {
      // The active claim remains present while we publish the receipt. A crash
      // before unlink quarantines this UUID despite the new receipt.
      const existing = await readReusableReceipt(this.directory, this.nodeHex);
      const confirmedTxTime = maxBigInt(
        this.confirmedTxTime,
        existing ? BigInt(existing.confirmedTxTime) : 0n,
        runtimeHighWater,
      );
      await writeReusableReceipt(this.directory, this.nodeHex, confirmedTxTime);
      await removeOwnedActiveClaim(this.directory, this.claim);
      this.finished = true;
    } catch (error) {
      throw new Error(`Node foreground lease handoff failed: ${asError(error).message}`);
    }
  }

  async retire(): Promise<void> {
    if (this.finished) return;
    try {
      await writeRetiredReceipt(this.directory, this.nodeHex);
      await removeOwnedActiveClaim(this.directory, this.claim);
      this.finished = true;
    } catch (error) {
      throw new Error(`Node foreground lease retirement failed: ${asError(error).message}`);
    }
  }
}

function leaseDirectory(options: NodeForegroundNodeLeaseOptions): string {
  const namespace = createHash("sha256")
    .update(JSON.stringify([options.appId, options.env, options.authScope]))
    .digest("hex");
  return resolve(process.cwd(), ".jazz", "foreground-node-leases", "v1", namespace);
}

async function ensureDirectories(directory: string): Promise<void> {
  await Promise.all(
    [
      directory,
      activeDirectory(directory),
      reusableDirectory(directory),
      retiredDirectory(directory),
    ].map((path) => mkdir(path, { recursive: true, mode: 0o700 })),
  );
}

async function reusableNodes(directory: string): Promise<string[]> {
  const entries = await readdir(reusableDirectory(directory), { withFileTypes: true });
  const candidates: string[] = [];
  for (const entry of entries) {
    if (!entry.isFile() || !NODE_RE.test(entry.name)) continue;
    if (
      !(await exists(activePath(directory, entry.name))) &&
      !(await exists(retiredPath(directory, entry.name)))
    ) {
      candidates.push(entry.name);
    }
  }
  return candidates.sort();
}

async function tryAcquireActiveClaim(directory: string, node: string): Promise<ActiveClaim | null> {
  if (!NODE_RE.test(node)) throw new Error("Invalid Node foreground lease node");
  const claim: ActiveClaim = { format: FORMAT, node, token: randomUUID() };
  try {
    const file = await open(activePath(directory, node), "wx", 0o600);
    try {
      await file.writeFile(`${JSON.stringify(claim)}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    await syncDirectory(activeDirectory(directory));
    return claim;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return null;
    throw error;
  }
}

async function removeOwnedActiveClaim(directory: string, claim: ActiveClaim): Promise<void> {
  const path = activePath(directory, claim.node);
  const actual = parseActiveClaim(await readFile(path, "utf8"));
  if (actual.token !== claim.token || actual.node !== claim.node) {
    throw new Error("Node foreground lease active claim is no longer owned by this runtime");
  }
  await unlink(path);
  await syncDirectory(activeDirectory(directory));
}

async function leaveClaimQuarantined(_claim: ActiveClaim): Promise<void> {
  // Intentionally empty: an unremoved active claim is the durable quarantine.
}

async function readReusableReceipt(
  directory: string,
  node: string,
): Promise<ReusableReceipt | null> {
  try {
    return parseReusableReceipt(await readFile(reusablePath(directory, node), "utf8"), node);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw new Error(`Invalid Node foreground lease receipt: ${asError(error).message}`);
  }
}

async function writeReusableReceipt(
  directory: string,
  node: string,
  confirmedTxTime: bigint,
): Promise<void> {
  const receipt: ReusableReceipt = {
    format: FORMAT,
    node,
    confirmedTxTime: confirmedTxTime.toString(),
  };
  await writeFileAtomically(reusablePath(directory, node), `${JSON.stringify(receipt)}\n`);
}

async function writeRetiredReceipt(directory: string, node: string): Promise<void> {
  const path = retiredPath(directory, node);
  try {
    const file = await open(path, "wx", 0o600);
    try {
      await file.writeFile(`${JSON.stringify({ format: FORMAT, node })}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    await syncDirectory(retiredDirectory(directory));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
}

async function writeFileAtomically(destination: string, value: string): Promise<void> {
  const temporary = join(dirname(destination), `.${randomUUID()}.tmp`);
  const file = await open(temporary, "wx", 0o600);
  try {
    await file.writeFile(value);
    await file.sync();
  } finally {
    await file.close();
  }
  await rename(temporary, destination);
  await syncDirectory(dirname(destination));
}

async function syncDirectory(path: string): Promise<void> {
  const directory = await open(path, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

async function exists(path: string): Promise<boolean> {
  try {
    await readFile(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function parseReusableReceipt(value: string, expectedNode: string): ReusableReceipt {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("receipt is not JSON");
  }
  const receipt = parsed as Partial<ReusableReceipt>;
  if (
    !receipt ||
    receipt.format !== FORMAT ||
    receipt.node !== expectedNode ||
    typeof receipt.confirmedTxTime !== "string" ||
    !/^(0|[1-9][0-9]*)$/.test(receipt.confirmedTxTime) ||
    !isTxTime(BigInt(receipt.confirmedTxTime))
  )
    throw new Error("receipt has an invalid shape");
  return receipt as ReusableReceipt;
}

function parseActiveClaim(value: string): ActiveClaim {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("active claim is not JSON");
  }
  const claim = parsed as Partial<ActiveClaim>;
  if (
    !claim ||
    claim.format !== FORMAT ||
    typeof claim.node !== "string" ||
    !NODE_RE.test(claim.node) ||
    typeof claim.token !== "string" ||
    !/^[0-9a-f-]{36}$/i.test(claim.token)
  ) {
    throw new Error("active claim has an invalid shape");
  }
  return claim as ActiveClaim;
}

function activeDirectory(directory: string): string {
  return join(directory, ACTIVE_DIRECTORY);
}
function reusableDirectory(directory: string): string {
  return join(directory, REUSABLE_DIRECTORY);
}
function retiredDirectory(directory: string): string {
  return join(directory, RETIRED_DIRECTORY);
}
function activePath(directory: string, node: string): string {
  return join(activeDirectory(directory), node);
}
function reusablePath(directory: string, node: string): string {
  return join(reusableDirectory(directory), node);
}
function retiredPath(directory: string, node: string): string {
  return join(retiredDirectory(directory), node);
}
function maxBigInt(...values: bigint[]): bigint {
  return values.reduce((max, value) => (value > max ? value : max), 0n);
}
function isTxTime(value: bigint): boolean {
  return value >= 0n && value <= MAX_TX_TIME;
}
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
