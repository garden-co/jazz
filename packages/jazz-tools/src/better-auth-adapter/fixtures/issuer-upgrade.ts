// Frozen app files from the auth-betterauth-chat example: `legacy` is its
// Better Auth 1.7.1 schema (main5c3c, accounts carry `issuer`), `current` is
// PR4679@4365de09, which ships the legacy version in its history and the
// migration dropping `issuer`. Tests pin their canonical schema identities to
// the hashes the examples shipped. Node only: loads jazz-napi.
import { readFileSync } from "node:fs";

const read = (name: string) => readFileSync(new URL(name, import.meta.url), "utf8");

/** jazz.app.json of the issuer-era app, with no history. */
export const legacyAppFile = read("./issuer-upgrade/legacy.app.json");
/** jazz.app.json of the current app: the legacy version in history, and the migration from it. */
export const currentAppFile = read("./issuer-upgrade/jazz.app.json");

/** The hashes every example shipped for the issuer-era and current schemas. */
export const legacyHash = "8acd8a85eef5015347cfc6ee0024ea14148aebb59869345c4d53a8e390bc6b88";
export const currentHash = "c95f3dba180615f83246729f0dceceab4b384ad00a92f30c59964f725524d822";

/** Deny-all policies for every table of both versions. */
export const denyAllPermissions = {
  format: 1,
  tables: Object.fromEntries(
    [
      "better_auth_user",
      "better_auth_session",
      "better_auth_account",
      "better_auth_verification",
      "better_auth_jwks",
      "messages",
    ].map((name) => [name, { read: "false", insert: "false", update: "false", delete: "false" }]),
  ),
};
