import { entropyToMnemonic, mnemonicToEntropy } from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english.js";
import { formatSecret, parseSecret } from "./secret.js";

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

function normalize(phrase: string): string[] {
  const trimmed = phrase.normalize("NFKD").trim();
  if (trimmed.length === 0) return [];
  return trimmed.toLowerCase().split(/\s+/u);
}

export const RecoveryPhrase = {
  fromSecret(secret: string): string {
    if (typeof secret !== "string") {
      throw new RecoveryPhraseError("invalid-secret", "Secret must be a string");
    }
    try {
      return entropyToMnemonic(parseSecret(secret), wordlist);
    } catch {
      throw new RecoveryPhraseError(
        "invalid-secret",
        "Secret is not a canonical device account key",
      );
    }
  },

  toSecret(phrase: string): string {
    return formatSecret(RecoveryPhrase.toBytes(phrase));
  },

  /** The 24-word phrase of 32 secret bytes (the same words as {@link fromSecret}). */
  fromBytes(bytes: Uint8Array): string {
    if (!(bytes instanceof Uint8Array) || bytes.length !== 32) {
      throw new RecoveryPhraseError("invalid-secret", "A recovery secret is 32 bytes");
    }
    return entropyToMnemonic(bytes, wordlist);
  },

  /** The 32 secret bytes of a 24-word phrase, checked as {@link toSecret} checks it. */
  toBytes(phrase: string): Uint8Array {
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
          `Word ${i + 1} is not in the recovery word list`,
        );
      }
    }
    try {
      return mnemonicToEntropy(words.join(" "), wordlist);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (/checksum/i.test(message)) {
        throw new RecoveryPhraseError("invalid-checksum", "Recovery phrase checksum is invalid");
      }
      throw new RecoveryPhraseError("invalid-length", message);
    }
  },
};
