import { base58 } from "@scure/base";
import { JsonValue } from "cojson";
import { PureJSCrypto } from "cojson/src/crypto/PureJSCrypto.js";
import { Signature, SignerID, SignerSecret } from "cojson/src/crypto/crypto.js";
import { stableStringify } from "cojson/src/jsonStringify.js";
import { Ed } from "react-native-quick-crypto";

export class RNQuickCrypto extends PureJSCrypto {
  ed: Ed;

  constructor() {
    super();
    this.ed = new Ed("ed25519", {});
  }

  static async create(): Promise<RNQuickCrypto> {
    return new RNQuickCrypto();
  }

  newEd25519SigningKey(): Uint8Array {
    this.ed.generateKeyPairSync();
    return new Uint8Array(this.ed.getPublicKey());
  }

  getSignerID(secret: SignerSecret): SignerID {
    return `signer_z${base58.encode(
      new Uint8Array(this.ed.getPublicKey()),
      // base58.decode(secret.substring("signerSecret_z".length)),
    )}`;
  }

  sign(secret: SignerSecret, message: JsonValue): Signature {
    const buf = Buffer.from(stableStringify(message));
    const signature = new Uint8Array(this.ed.signSync(buf.buffer));
    return `signature_z${base58.encode(signature)}`;
  }

  verify(signature: Signature, message: JsonValue, id: SignerID): boolean {
    const buf = Buffer.from(stableStringify(message));
    return this.ed.verifySync(
      base58.decode(signature.substring("signature_z".length)),
      buf.buffer,
      // base58.decode(id.substring("signer_z".length)),
    );
  }
}
