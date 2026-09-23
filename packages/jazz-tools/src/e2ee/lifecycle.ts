import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { WasmSchema } from "../drivers/types.js";
import { accountRegistry, exportLocalFirstSecret } from "../accounts/enrollment.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import { openRecoveryMaterial, protectRecoveryMaterial } from "./recovery-protection.js";
import { E2eeRecoveryError } from "./recovery-error.js";
import type { AccountHandle } from "../accounts/state.js";
import type { Db } from "../runtime/db.js";
import { deviceRequestApp, deviceRequestSchema } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { encodeEnvelope } from "./envelope.js";
import type { CryptoMechanism } from "./envelope.js";
import { localDevice } from "./local-device.js";
import { firstAccountEpoch } from "./first-epoch.js";
import { DeviceApproval } from "./device-approval.js";
import {
  prefetchPublicMembershipHistory,
  readPublicMembershipHistory,
  replayAccountMembership,
} from "./public-membership.js";
import type { JazzCrypto } from "./types.js";

export type E2eeConfig = {
  /** Explicit application including the lifecycle managed tables. */
  app?: DeviceTables | { readonly wasmSchema: WasmSchema };
  /** Dedicated local key storage. Do not reuse the account-selection store value. */
  store: AccountStore;
  crypto?: JazzCrypto;
};
export type DeviceInfo = Readonly<{
  id: string;
  /** Verified approval membership, independent of this caller's key access. */
  state: "pending" | "active" | "revoked";
  /** This caller verified the current key-delivery chain; not proof of remote key possession. */
  keyReadiness: "verified" | "not-verified";
  publicKey: Uint8Array;
  mechanism: CryptoMechanism;
}>;

const contexts = new WeakMap<Db, E2ee>();

export type RecoveryStatus = Readonly<{
  configured: boolean;
  account: {
    epochId: string | null;
    /** Accepted active registrations, not evidence that a device is online. */
    activeDeviceIds: string[];
    recoveryRootIds: string[];
    validation: "not-checked" | "validated";
    validatedRootId?: string;
  };
}>;

/** @internal Only verified account-context creation binds lifecycle state. */
export function attachE2ee(db: Db, account: AccountHandle, config: E2eeConfig, env: string): void {
  contexts.set(db, new E2ee(db, account, config, env));
}

/** @internal Schema objects never own account keys or lifecycle state. */
export function e2eeForDb(db: Db): E2ee {
  const e2ee = contexts.get(db);
  if (!e2ee) throw new Error("E2EE requires an authenticated Db with dedicated local key storage");
  return e2ee;
}

/** First-device activation; approval and rotation are separate lifecycle operations. */
export class E2ee {
  private approval: DeviceApproval | undefined;
  private closed = false;
  private preparation: Promise<void> | undefined;
  private readonly scope: string;
  private readonly app: DeviceTables;

  readonly recovery = {
    status: async (material?: string): Promise<RecoveryStatus> => {
      this.assertOpen();
      if (material !== undefined) return this.inspectRecovery(material);
      await prefetchPublicMembershipHistory(this.db, this.account.id, this.app);
      const read = await exclusiveE2eeTransaction(this.db, (tx) =>
        readPublicMembershipHistory(tx, this.account.id, this.app),
      );
      const history = await read.wait({ tier: "global" });
      this.assertOpen();
      let membership: Awaited<ReturnType<typeof replayAccountMembership>> | undefined;
      if (history.roots.rows.length) {
        const signer =
          this.config.crypto?.deviceSigner ??
          (await (await import("./browser.js")).createBrowserDeviceSigner());
        membership = await replayAccountMembership(history, this.scope, signer);
        this.assertOpen();
      }
      if (
        membership?.recoveryRoots.length &&
        this.account.identity.issuer === "urn:jazz:local-first"
      )
        return this.withProtectedRecovery((value) => this.inspectRecovery(value));
      // Registration is evidence of configuration, never of retained recovery secrets.
      return {
        configured: !!membership?.recoveryRoots.length,
        account: {
          epochId: membership?.epochId ?? null,
          activeDeviceIds: [...(membership?.active ?? [])].sort(),
          recoveryRootIds: membership?.recoveryRoots.map((root) => root.id).sort() ?? [],
          validation: "not-checked",
        },
      };
    },
    use: (material?: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        if (material !== undefined) {
          await this.approval!.useRecovery(material);
        } else await this.restoreProtectedRecovery();
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    create: (): { wait(): Promise<{ material: string }> } => {
      const completion = (async () => {
        await this.prepare();
        const result = await this.approval!.createRecovery();
        if (this.account.identity.issuer === "urn:jazz:local-first") {
          const secret = parseAuthSecret(exportLocalFirstSecret(this.account));
          try {
            const cipher =
              this.config.crypto?.cellCipher ??
              (await (await import("./browser.js")).createBrowserCellCipher());
            const rootId: string = JSON.parse(result.material).rootId;
            const target = { application: this.scope, accountId: this.account.id, rootId };
            const material = await protectRecoveryMaterial(cipher, secret, target, result.material);
            if ((await openRecoveryMaterial(cipher, secret, target, material)) !== result.material)
              throw new Error("Invalid E2EE recovery protector output");
            this.assertOpen();
            await this.db
              .insert(this.app.__e2ee_recovery_protectors, { rootId, material })
              .wait({ tier: "global" });
            this.assertOpen();
          } finally {
            secret.fill(0);
          }
        }
        return result;
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  private async inspectRecovery(material: string): Promise<RecoveryStatus> {
    const keys =
      this.config.crypto?.keyEnvelope ??
      (await (await import("./browser.js")).createBrowserKeyEnvelope());
    const signer =
      this.config.crypto?.deviceSigner ??
      (await (await import("./browser.js")).createBrowserDeviceSigner());
    this.assertOpen();
    const reader = new DeviceApproval(
      this.db,
      this.account.id,
      this.scope,
      keys,
      signer,
      () => this.assertOpen(),
      this.app,
    );
    const account = await reader.inspectRecovery(material);
    this.assertOpen();
    return { configured: true, account };
  }

  private restoreProtectedRecovery(): Promise<void> {
    return this.withProtectedRecovery(async (material) => {
      await this.approval!.useRecovery(material);
      this.assertOpen();
    });
  }

  private async withProtectedRecovery<T>(consume: (material: string) => Promise<T>): Promise<T> {
    const secret = parseAuthSecret(exportLocalFirstSecret(this.account));
    try {
      const cipher =
        this.config.crypto?.cellCipher ??
        (await (await import("./browser.js")).createBrowserCellCipher());
      const rows = await this.db.all(this.app.__e2ee_recovery_protectors, { tier: "edge" });
      let failure: unknown = new E2eeRecoveryError("recovery-protector-missing");
      for (const row of rows) {
        this.assertOpen();
        let material: string;
        try {
          material = await openRecoveryMaterial(
            cipher,
            secret,
            { application: this.scope, accountId: this.account.id, rootId: row.rootId },
            row.material,
          );
          if (JSON.parse(material).rootId !== row.rootId)
            throw new Error("E2EE recovery protector root mismatch");
        } catch {
          this.assertOpen();
          failure = new E2eeRecoveryError("recovery-protector-unusable");
          continue;
        }
        try {
          return await consume(material);
        } catch (error) {
          this.assertOpen();
          failure = error;
        }
      }
      throw failure;
    } finally {
      secret.fill(0);
    }
  }

  readonly devices = {
    list: async (): Promise<DeviceInfo[]> => {
      await this.prepare();
      const { approved, verified, revoked } = await this.approval!.deviceStates();
      const rows = await this.db.all(this.app.__e2ee_device_requests, { tier: "edge" });
      this.assertOpen();
      return rows.map((row) => ({
        id: row.id,
        state: revoked.has(row.id) ? "revoked" : approved.has(row.id) ? "active" : "pending",
        keyReadiness: verified.has(row.id) && !revoked.has(row.id) ? "verified" : "not-verified",
        publicKey: Uint8Array.from(row.publicKey),
        mechanism: { id: row.mechanism, version: row.version },
      }));
    },
    approve: (deviceId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        await this.approval!.approve(deviceId);
      })();
      // The returned wait handle owns deferred errors, even when attached later.
      completion.catch(() => {});
      return { wait: () => completion };
    },
    revoke: (deviceId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        await this.approval!.revoke(deviceId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  constructor(
    private readonly db: Db,
    private readonly account: AccountHandle,
    private readonly config: E2eeConfig,
    private readonly env: string,
  ) {
    const app = config.app ?? deviceRequestApp;
    for (const name of Object.keys(deviceRequestSchema)) {
      if (!(name in app)) {
        throw new Error(`E2EE application is missing managed table "${name}"`);
      }
    }
    this.app = app as DeviceTables;
    this.scope = JSON.stringify([accountRegistry(account), env, account.id]);
    db.onShutdown(() => {
      this.closed = true;
    });
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("Cannot operate on a closed E2EE context");
    accountRegistry(this.account); // Reject logout, including during asynchronous preparation.
  }

  private async prepare(): Promise<void> {
    this.assertOpen();
    await (this.preparation ??= this.prepareRequest().catch((error) => {
      this.preparation = undefined;
      throw error;
    }));
    this.assertOpen();
  }

  private async prepareRequest(): Promise<void> {
    const envelope =
      this.config.crypto?.keyEnvelope ??
      (await (await import("./browser.js")).createBrowserKeyEnvelope());
    encodeEnvelope(envelope.mechanism, new Uint8Array());
    const signer =
      this.config.crypto?.deviceSigner ??
      (await (await import("./browser.js")).createBrowserDeviceSigner());
    encodeEnvelope(signer.mechanism, new Uint8Array());
    const device = await localDevice(this.config.store, this.scope, envelope, signer, () =>
      this.assertOpen(),
    );
    try {
      this.assertOpen();
      const requests = this.app.__e2ee_device_requests;
      // Once online preparation starts, complete it or reject so it can retry.
      // A later disconnect must not cache skipped enrolment as successful.
      {
        const query = requests.where({ id: device.id });
        let row = await this.db.one(query, { tier: "edge" });
        if (!row) {
          this.assertOpen();
          try {
            row = await this.db
              .insert(
                requests,
                {
                  publicKey: device.publicKey,
                  mechanism: envelope.mechanism.id,
                  version: envelope.mechanism.version,
                  challenge: device.challenge,
                  signingPublicKey: device.signing.publicKey,
                  signingMechanism: signer.mechanism.id,
                  signingVersion: signer.mechanism.version,
                },
                { id: device.id },
              )
              .wait({ tier: "edge" });
          } catch (error) {
            // Another context may have published this same durable proposal.
            row = await this.db.one(query, { tier: "edge" });
            if (!row) throw error;
          }
        }
        this.assertOpen();
        if (
          row.mechanism !== envelope.mechanism.id ||
          row.version !== envelope.mechanism.version ||
          row.signingMechanism !== signer.mechanism.id ||
          row.signingVersion !== signer.mechanism.version ||
          !sameBytes(row.signingPublicKey, device.signing.publicKey) ||
          !sameBytes(row.publicKey, device.publicKey) ||
          !sameBytes(row.challenge, device.challenge)
        )
          throw new Error("E2EE device request does not match the locally retained key");
        const publicKeys = this.app.__e2ee_device_keys;
        const publicQuery = publicKeys.where({ deviceId: device.id });
        if (!(await this.db.one(publicQuery, { tier: "edge" }))) {
          this.assertOpen();
          try {
            await this.db
              .insert(publicKeys, {
                deviceId: device.id,
                publicKey: row.publicKey,
                mechanism: row.mechanism,
                version: row.version,
                signingPublicKey: row.signingPublicKey,
                signingMechanism: row.signingMechanism,
                signingVersion: row.signingVersion,
              })
              .wait({ tier: "edge" });
          } catch (error) {
            // Concurrent contexts may publish the same immutable, policy-checked keys.
            if (!(await this.db.one(publicQuery, { tier: "edge" }))) throw error;
          }
        }
        this.assertOpen();
        await firstAccountEpoch(
          this.db,
          this.account.id,
          this.scope,
          device,
          envelope,
          () => this.assertOpen(),
          this.app,
        );
      }
      // Reconnection enrols once without adding another responder or key owner.
      if (this.approval) return;
      const retainedKey = device.privateKey.slice();
      const retainedSigningKey = device.signing.privateKey.slice();
      this.db.onShutdown(() => {
        retainedKey.fill(0);
        retainedSigningKey.fill(0);
      });
      const loadDevice = async () => ({
        ...device,
        privateKey: retainedKey.slice(),
        signing: { ...device.signing, privateKey: retainedSigningKey.slice() },
      });
      this.approval = new DeviceApproval(
        this.db,
        this.account.id,
        this.scope,
        envelope,
        signer,
        () => this.assertOpen(),
        this.app,
        { id: device.id, load: loadDevice },
      );
    } finally {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}
