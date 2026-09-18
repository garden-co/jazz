import type { AccountStore } from "../accounts/persistence.js";
import type { WasmSchema } from "../drivers/types.js";
import { accountRegistry } from "../accounts/enrollment.js";
import type { AccountHandle } from "../accounts/state.js";
import type { Db } from "../runtime/db.js";
import { deviceRequestApp, deviceRequestSchema } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { encodeEnvelope } from "./envelope.js";
import type { CryptoMechanism } from "./envelope.js";
import { localDevice } from "./local-device.js";
import { firstAccountEpoch } from "./first-epoch.js";
import { DeviceApproval } from "./device-approval.js";
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
