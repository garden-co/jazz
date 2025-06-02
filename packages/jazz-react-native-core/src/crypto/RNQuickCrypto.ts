import { base58 } from "@scure/base";
import {
  JsonValue,
  Stringified,
  base64URLtoBytes,
  bytesToBase64url,
} from "cojson";
import { CojsonInternalTypes, cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto"; // Importing from dist to not rely on the exports field
import { Ed, xsalsa20 } from "react-native-quick-crypto";
const { stableStringify } = cojsonInternals;

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
    return new Uint8Array(this.ed.getPrivateKey());
  }

  sign(
    secret: CojsonInternalTypes.SignerSecret,
    message: JsonValue,
  ): CojsonInternalTypes.Signature {
    const signature = new Uint8Array(
      this.ed.signSync(
        cojsonInternals.textEncoder.encode(stableStringify(message)),
        base58.decode(secret.substring("signerSecret_z".length)),
      ),
    );
    return `signature_z${base58.encode(signature)}`;
  }

  verify(
    signature: CojsonInternalTypes.Signature,
    message: JsonValue,
    id: CojsonInternalTypes.SignerID,
  ): boolean {
    return this.ed.verifySync(
      base58.decode(signature.substring("signature_z".length)),
      cojsonInternals.textEncoder.encode(stableStringify(message)),
      base58.decode(id.substring("signer_z".length)),
    );
  }

  encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): CojsonInternalTypes.Encrypted<T, N> {
    const keySecretBytes = base58.decode(
      keySecret.substring("keySecret_z".length),
    );
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const plaintext = cojsonInternals.textEncoder.encode(
      stableStringify(value),
    );
    const ciphertext = xsalsa20(keySecretBytes, nOnce, plaintext);
    return `encrypted_U${bytesToBase64url(ciphertext)}` as CojsonInternalTypes.Encrypted<
      T,
      N
    >;
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: CojsonInternalTypes.Encrypted<T, N>,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    const keySecretBytes = base58.decode(
      keySecret.substring("keySecret_z".length),
    );
    const nOnce = this.generateJsonNonce(nOnceMaterial);

    const ciphertext = base64URLtoBytes(
      encrypted.substring("encrypted_U".length),
    );
    const plaintext = xsalsa20(keySecretBytes, nOnce, ciphertext);

    return cojsonInternals.textDecoder.decode(plaintext) as Stringified<T>;
  }
}
