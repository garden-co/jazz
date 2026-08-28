import { entropyToMnemonic, mnemonicToEntropy } from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english.js";
import { formatAuthSecret, parseAuthSecret } from "./auth-secret-codec.js";

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
      return entropyToMnemonic(parseAuthSecret(secret), wordlist);
    } catch {
      throw new RecoveryPhraseError("invalid-secret", "Secret is not a canonical Jazz auth secret");
    }
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
    return formatAuthSecret(bytes);
  },
};
