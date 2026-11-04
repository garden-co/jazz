import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { legacyDefinition, currentDefinition } from "./fixtures/issuer-upgrade-schema.js";
import { migration as m } from "../migration-namespace.js";
import { structuralSchemaHash } from "../dev/schema-utils.js";
import { assertMigrationMatchesCanonicalBundle, serializeForwardLenses } from "../dev/catalogue.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import type { WasmSchema } from "../drivers/types.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
// Original, shipped pre-1.7.16 schema identities from main5c3c; the fourth
// coordinate pins ALL predecessor metadata, not just structural hash inputs.
const examples = [
  [
    "examples/auth-betterauth-chat/migrations",
    "8acd8a85eef5015347cfc6ee0024ea14148aebb59869345c4d53a8e390bc6b88",
    "c95f3dba180615f83246729f0dceceab4b384ad00a92f30c59964f725524d822",
    "c00406b3b2a7e83583c0e779d407f5cdb883377f3fdaf86534eaf988ae9e0372",
  ],
  [
    "examples/band-book/apps/nextjs-betterauth/migrations",
    "6b7728f29ab763b9398f91988f71c10d54826c1d5217d18787dc54bc7dda6176",
    "bee680a0972b72b7c83d95395a1667bff87cba7516d36cecf613a53755bbf8e5",
    "706070f83202da74dbec6e922afbf1b9518a68c5c743c256721e421b264d735f",
  ],
  [
    "examples/band-chat/apps/nextjs-betterauth/migrations",
    "22763a78d819642815e2ed6b1d3d0944f01f5415c59f7e0955cf5a734a2c522a",
    "c05382c3683b3e0390ca0bdeb64182fadd80af33a2056b314645abb06c3a49b0",
    "1d164397ee2e9ca640580849fdd28b4571600a6a67a78c5cd9fd375f160a21fc",
  ],
  [
    "examples/band-chat/apps/react-hono-betterauth/migrations",
    "22763a78d819642815e2ed6b1d3d0944f01f5415c59f7e0955cf5a734a2c522a",
    "c05382c3683b3e0390ca0bdeb64182fadd80af33a2056b314645abb06c3a49b0",
    "1d164397ee2e9ca640580849fdd28b4571600a6a67a78c5cd9fd375f160a21fc",
  ],
  [
    "examples/big-label/migrations",
    "b6640bee549149d8eaf9f9f8e7cddf486d6b3bd50cee2ddbb06a855e1980aa04",
    "d986c626f864b1dd9e3e6a642819200d76f0e67d63da84553f95ceaa2e2ef207",
    "a3550b418a8a7af3516b984d2d131085e92833fe9b712825fd144c93236d0d69",
  ],
  [
    "examples/jamazon-warehouse/migrations",
    "7ce960ad34dc4f77c92e07727acc3113ccf803934671a1fe2f46cf736d2641ce",
    "36b03f261e65be21093aa294715b621b753dbc531e003f1487436c1946f174f9",
    "ee6ac1df64d1439ba2d2ce3052209386f423e5017b1da7eb0bc481aa18056419",
  ],
  [
    "examples/jamazon/apps/nextjs-betterauth/migrations",
    "5806d5c6e98c33ae41c6ba5b3da6cca0ca72d69467521c5381c1babe56adf6b0",
    "f7318f0be5fcbf2065324e4f545c96323edfa4e007186fb89eabff519ca370d8",
    "43719d926aaacbef65f858cba499684ab927e53c858dcf19c8322415f29d294a",
  ],
  [
    "examples/music-agent/apps/nextjs/migrations",
    "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
    "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
    "27cdab18ad2880fe259a2ceb513be8c367ce5163583edbf53b364296d4a681f8",
  ],
  [
    "examples/music-agent/apps/ts-hono/migrations",
    "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
    "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
    "27cdab18ad2880fe259a2ceb513be8c367ce5163583edbf53b364296d4a681f8",
  ],
  [
    "examples/poster-shop/apps/nextjs-betterauth/migrations",
    "55a9ac8b000f23198f161d0e64ea96e10fc608de6c8edabad0925d39c472badc",
    "c2c80e4c3ecf667af8dd57d937fd72d5b6198e85d36753987ba3047838fb9fd9",
    "4598153be094094924fb224e6952c89ef0ade1b92d887ab85e6a0a177fb199f8",
  ],
  [
    "examples/record-player/apps/next-betterauth/migrations",
    "0f726114893031ae7b5326c91ab91869f9713d0d54b9a9e753957080ee66370c",
    "fa66c6bd2a11cdf22cc6efb1e5c7737070b322de45163e636068309507671292",
    "c1628830a890ae295f1d7904eafc993498b7d05a061223ca953b7a529ca8724f",
  ],
  [
    "examples/stage-plan/apps/sveltekit-hybrid/migrations",
    "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
    "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
    "a986478f2f45446637f37aca7f8210aa8e5c499ee9fc7daf8444baab6bb35d80",
  ],
  [
    "examples/stage-plan/apps/ts-effect-betterauth/migrations",
    "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
    "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
    "a986478f2f45446637f37aca7f8210aa8e5c499ee9fc7daf8444baab6bb35d80",
  ],
  [
    "examples/wequencer/apps/next-betterauth/migrations",
    "a0d57d731cd5c19c2810769738e14ba99c2b0cd2665e4b11e44d3e73d4502b67",
    "c1f2524cc44f9ca8d099559c30f3b51e230d846119cfe3e92ced05962a7fa6b3",
    "6e938c531dabae6716fa2e841f0ea583048e52870553c21e46e3cbc778e3d7a0",
  ],
] as const;
const loadJson = (path: string) => JSON.parse(readFileSync(path, "utf8"));
function bundleAt(directory: string): ClientMigrations {
  const source = readFileSync(join(root, directory, "index.ts"), "utf8");
  return JSON.parse(
    source.slice(
      source.indexOf("{", source.indexOf("export const migrations")),
      source.lastIndexOf(";"),
    ),
  );
}
function migrationAt(directory: string) {
  const source = readFileSync(join(root, directory, "remove-better-auth-issuer.ts"), "utf8");
  // Same public builders as the normal schema loader, without loading unrelated
  // native backend exports. This checks the real migration; it simulates no DB.
  return new Function(
    "s",
    "m",
    source
      .replace('import { schema as s, migration as m } from "jazz-tools";', "")
      .replace("export default", "return"),
  )(s, m) as ReturnType<typeof m.defineMigration>;
}

describe("shipped Better Auth issuer-removal histories", () => {
  it.each(examples)(
    "keeps the exact connected upgrade for %s",
    (directory, fromHash, toHash, snapshotDigest) => {
      const from: WasmSchema = loadJson(
        join(root, directory, "snapshots", fromHash.slice(0, 12) + ".json"),
      );
      const to: WasmSchema = loadJson(
        join(root, directory, "snapshots", toHash.slice(0, 12) + ".json"),
      );
      expect(structuralSchemaHash(from)).toBe(fromHash);
      expect(structuralSchemaHash(to)).toBe(toHash);
      expect(createHash("sha256").update(JSON.stringify(from)).digest("hex")).toBe(snapshotDigest);
      const bundle = bundleAt(directory);
      expect(bundle.schemas).toHaveLength(2);
      expect(bundle.schemas.find((v) => v.hash === fromHash)?.schema).toEqual({ tables: from });
      expect(bundle.schemas.find((v) => v.hash === toHash)?.schema).toEqual({ tables: to });
      const migration = migrationAt(directory);
      expect(() =>
        assertMigrationMatchesCanonicalBundle(migration, {
          fromHash,
          toHash,
          fromSchema: from,
          toSchema: to,
        }),
      ).not.toThrow();
      expect(bundle.migrations).toEqual([
        { fromHash, toHash, forward: serializeForwardLenses(migration.forward) },
      ]);
      expect(migration.forward).toEqual([
        {
          table: "better_auth_account",
          operations: [{ type: "drop", column: "issuer", sqlType: "TEXT", value: "" }],
        },
      ]);
      const { better_auth_account: account, ...unchanged } = from;
      expect(to).toEqual({
        ...unchanged,
        better_auth_account: {
          ...account,
          columns: account.columns.filter((column) => column.name !== "issuer"),
        },
      });
      // Password/provider/account/user coordinates remain in the migrated view.
      for (const name of ["password", "providerId", "accountId", "userId"]) {
        expect(to.better_auth_account.columns.find((column) => column.name === name)).toEqual(
          account.columns.find((column) => column.name === name),
        );
      }
    },
  );

  it("compiles the frozen real example definitions to the shipped upgrade endpoints", () => {
    expect(
      structuralSchemaHash(s.defineApp(legacyDefinition, { migrations: "none" }).wasmSchema),
    ).toBe(examples[0][1]);
    expect(
      structuralSchemaHash(s.defineApp(currentDefinition, { migrations: "none" }).wasmSchema),
    ).toBe(examples[0][2]);
  });

  it("refuses the structural issuer removal without a migration lens", () => {
    const [directory, fromHash, toHash] = examples[0];
    const fromSchema = loadJson(
      join(root, directory, "snapshots", fromHash.slice(0, 12) + ".json"),
    );
    const toSchema = loadJson(join(root, directory, "snapshots", toHash.slice(0, 12) + ".json"));
    const migration = migrationAt(directory);
    expect(() =>
      assertMigrationMatchesCanonicalBundle(
        { ...migration, forward: [] },
        { fromHash, toHash, fromSchema, toSchema },
      ),
    ).toThrow(/requires a migration/);
  });

  it("refuses attaching the old hash to a witness with its issuer already removed", () => {
    const [directory, fromHash, toHash] = examples[0];
    const toSchema = loadJson(join(root, directory, "snapshots", toHash.slice(0, 12) + ".json"));
    expect(() =>
      assertMigrationMatchesCanonicalBundle(migrationAt(directory), {
        fromHash,
        toHash,
        fromSchema: toSchema,
        toSchema,
      }),
    ).toThrow(/from schema witness/);
  });
});
