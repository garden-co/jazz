import { randomBytes } from "node:crypto";

export function generateLocalFirstSecret(): string {
  return randomBytes(32).toString("base64url");
}
