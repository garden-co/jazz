// Host-admitted storage-owner scope binding a durable client relay.
import { utf8, concatBytes } from "../../support/bytes.js";
import {
  type AuthorSubject,
  authorEquals,
  blake3DeriveKey,
} from "../../_extern/node-policy-peer.js";

function u64be(n: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(n));
  return b;
}

export class ClientRelayScope {
  private constructor(
    readonly storageOwner: string,
    readonly admittedSession: AuthorSubject | undefined,
  ) {}

  durableDigest(): Uint8Array {
    const owner = utf8(this.storageOwner);
    const parts = [u64be(owner.length), owner];
    if (this.admittedSession !== undefined) {
      const canonical = utf8(this.admittedSession.canonical());
      parts.push(Uint8Array.of(1), u64be(canonical.length), canonical);
    } else parts.push(Uint8Array.of(0));
    return blake3DeriveKey("jazz.scope-relay-repair-ledger.v1", concatBytes(...parts));
  }

  durableComponents(): [string, string | undefined] {
    return [this.storageOwner, this.admittedSession?.canonical()];
  }

  static fromAdmittedStorageOwner(
    storageOwner: string,
    admittedSession: AuthorSubject,
  ): ClientRelayScope {
    return new ClientRelayScope(storageOwner, admittedSession);
  }

  sameOwner(other: ClientRelayScope): boolean {
    return (
      this.storageOwner === other.storageOwner &&
      (this.admittedSession === undefined
        ? other.admittedSession === undefined
        : other.admittedSession !== undefined &&
          authorEquals(this.admittedSession, other.admittedSession))
    );
  }

  admitsSession(session: AuthorSubject): boolean {
    return this.admittedSession === undefined || authorEquals(this.admittedSession, session);
  }
}
