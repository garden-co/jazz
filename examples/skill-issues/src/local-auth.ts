import { randomBytes } from "node:crypto";
import { mintLocalFirstToken } from "jazz-napi";

export function generateLocalFirstSecret(): string {
  return randomBytes(32).toString("base64url");
}

export function createLocalFirstProof(secret: string): string {
  return mintLocalFirstToken(secret, "skill-issues-github", 60);
}
