import { entropyToMnemonic, mnemonicToEntropy } from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english.js";

export type RecoveryPhraseErrorCode =
  | "invalid-word"
  | "invalid-checksum"
  | "invalid-length"
  | "invalid-secret";

export class RecoveryPhraseError extends Error {
  readonly code: RecoveryPhraseErrorCode;
  constructor(code: RecoveryPhraseErrorCode, message: string) {
    super(message);
    this.name = "RecoveryPhraseError";
    this.code = code;
  }
}

const WORDSET = new Set(wordlist);

function base64urlToBytes(input: string): Uint8Array {
  const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalized.length % 4;
  const padded = padding === 0 ? normalized : normalized + "=".repeat(4 - padding);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function bytesToBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function normalize(phrase: string): string[] {
  const trimmed = phrase.trim();
  if (trimmed.length === 0) return [];
  return trimmed.toLowerCase().split(/\s+/u);
}

export const RecoveryPhrase = {
  fromSecret(secret: string): string {
    if (typeof secret !== "string") {
      throw new RecoveryPhraseError("invalid-secret", "Secret must be a string");
    }
    let bytes: Uint8Array;
    try {
      bytes = base64urlToBytes(secret);
    } catch {
      throw new RecoveryPhraseError("invalid-secret", "Secret is not valid base64url");
    }
    if (bytes.length !== 32) {
      throw new RecoveryPhraseError(
        "invalid-secret",
        `Secret must decode to 32 bytes, got ${bytes.length}`,
      );
    }
    return entropyToMnemonic(bytes, wordlist);
  },

  toSecret(phrase: string): string {
    if (typeof phrase !== "string") {
      throw new RecoveryPhraseError("invalid-length", "Phrase must be a string");
    }
    const words = normalize(phrase);
    if (words.length !== 24) {
      throw new RecoveryPhraseError("invalid-length", `Expected 24 words, got ${words.length}`);
    }
    for (let i = 0; i < words.length; i += 1) {
      const word = words[i]!;
      if (!WORDSET.has(word)) {
        throw new RecoveryPhraseError(
          "invalid-word",
          `Word ${i + 1} ("${word}") is not in the recovery word list`,
        );
      }
    }
    let bytes: Uint8Array;
    try {
      bytes = mnemonicToEntropy(words.join(" "), wordlist);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (/checksum/i.test(message)) {
        throw new RecoveryPhraseError("invalid-checksum", "Recovery phrase checksum is invalid");
      }
      throw new RecoveryPhraseError("invalid-length", message);
    }
    return bytesToBase64url(bytes);
  },
};
