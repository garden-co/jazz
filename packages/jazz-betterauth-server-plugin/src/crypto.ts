import * as crypto from "node:crypto";
import * as phc from "@phc/format";
import * as argon2 from "argon2";

/**
 * Splits ciphertext into its components.
 * @param encryptedString Ciphertext.
 * @returns The components of the ciphertext.
 */
const splitEncryptedString = (encryptedString: string) => {
  return {
    encryptedDataString: encryptedString.slice(56, -32),
    ivString: encryptedString.slice(0, 24),
    assocDataString: encryptedString.slice(24, 56),
    tagString: encryptedString.slice(-32),
  };
};

/**
 * Encrypts a string using the ChaCha20-Poly1305 algorithm.
 * @param plaintext Text to be encrypted.
 * @param key Encryption key.
 * @param encoding Ciphertext encoding format (defaults to 'hex').
 * @returns Ciphertext.
 */
const encryptString = (
  plaintext: string,
  key: crypto.CipherKey,
  encoding: BufferEncoding = "hex",
) => {
  const iv = crypto.randomBytes(12); // A 96-bit 'initialisation vector' (ie, nonce).
  const assocData = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv("chacha20-poly1305", key, iv, {
    authTagLength: 16,
  });
  cipher.setAAD(assocData, { plaintextLength: Buffer.byteLength(plaintext) });
  const encrypted = Buffer.concat([
    cipher.update(plaintext, "utf-8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();
  return (
    iv.toString(encoding) +
    assocData.toString(encoding) +
    encrypted.toString(encoding) +
    tag.toString(encoding)
  );
};

/**
 * Decrypts a string using the ChaCha20-Poly1305 algorithm.
 * @param ciphertext Ciphertext to be decrypted.
 * @param key Encryption key.
 * @param encoding Ciphertext encoding format (defaults to 'hex').
 * @returns Plaintext.
 */
const decryptString = (
  ciphertext: string,
  key: crypto.CipherKey,
  encoding: BufferEncoding = "hex",
) => {
  const { encryptedDataString, ivString, assocDataString, tagString } =
    splitEncryptedString(ciphertext);
  const iv = Buffer.from(ivString, encoding);
  const encryptedText = Buffer.from(encryptedDataString, encoding);
  const tag = Buffer.from(tagString, encoding);
  const decipher = crypto.createDecipheriv("chacha20-poly1305", key, iv, {
    authTagLength: 16,
  });
  decipher.setAAD(Buffer.from(assocDataString, encoding), {
    plaintextLength: encryptedDataString.length,
  });
  decipher.setAuthTag(Buffer.from(tag));
  const decrypted = decipher.update(encryptedText);
  return Buffer.concat([decrypted, decipher.final()]).toString();
};

/**
 * Encrypt text with a low-entropy secret ('password').
 * @param plaintext Plaintext to be encrypted.
 * @param password Password to be used for encryption.
 * @param encoding Encoding format for the ciphertext and salt (defaults to 'hex').
 * @returns Ciphertext and random salt.
 */
export const passwordEncrypt = async (
  plaintext: string,
  password: string,
  encoding: BufferEncoding = "hex",
): Promise<[string, string]> => {
  const salt = crypto.randomBytes(16);
  const passwordHash = await argon2.hash(password, { salt });
  const hash =
    phc.deserialize(passwordHash).hash ??
    Uint8Array.from(Buffer.from(passwordHash)).slice(0, 32);
  return [encryptString(plaintext, hash, encoding), salt.toString(encoding)];
};

/**
 * Decrypt text with a low-entropy secret ('password').
 * @param ciphertext Ciphertext to be decrypted.
 * @param password Password to be used for decryption.
 * @param salt Salt used to derive the encryption key.
 * @param encoding Encoding format for the ciphertext and salt (defaults to 'hex').
 * @returns Plaintext.
 */
export const passwordDecrypt = async (
  ciphertext: string,
  password: string,
  salt: string,
  encoding: BufferEncoding = "hex",
) => {
  const passwordHash = await argon2.hash(password, {
    salt: Buffer.from(salt, encoding),
  });
  const hash =
    phc.deserialize(passwordHash).hash ??
    Uint8Array.from(Buffer.from(passwordHash)).slice(0, 32);
  return decryptString(ciphertext, hash, encoding);
};
