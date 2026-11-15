import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { compileTestApp } from "../testing/test-app.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  currentAppFile,
  currentHash,
  legacyAppFile,
  legacyHash,
} from "./fixtures/issuer-upgrade.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
// The issuer-era (Better Auth 1.7.1) schema identities each example ships in
// its jazz.app.json history, and the current version after the issuer drop.
// The fourth coordinate pins ALL predecessor metadata (the compiled history
// version, which matched each example's shipped migrations/snapshots file
// column for column when the examples moved to files), not just structural
// hash inputs.
const examples = [
  [
    "examples/auth-betterauth-chat",
    "8acd8a85eef5015347cfc6ee0024ea14148aebb59869345c4d53a8e390bc6b88",
    "c95f3dba180615f83246729f0dceceab4b384ad00a92f30c59964f725524d822",
    "3a8e1b8eeae6f5bd4e4745174ec40fa3fae60274f63a5ad7ca5ca81900491a12",
  ],
  [
    "examples/band-book/apps/nextjs-betterauth",
    "f9c2e1ab9391434b67488cb23410ed340cd4836de7fbc3ce673ed2a56bc07f2c",
    "553979b0f2b20e85b22e5f83f56318fe11425287aea9f72eca6eb235c5512511",
    "d83aca516c9a476d36d0aaff1882bc16945ea5334154a55391eafdd051accb8d",
  ],
  [
    "examples/band-chat/apps/nextjs-betterauth",
    "0fe493411de2f896802f229b2d445a17fcb95794a8c492906c2ba22c1cf7a8d0",
    "2322e143bd2759b30f51c27f300fb37206653faf1142a5cc7b16fc9f821ff7a7",
    "badf8ee786d8d1893728554366a2791db8df1b8f7122ae15eec9c04cbd5a7794",
  ],
  [
    "examples/band-chat/apps/react-hono-betterauth",
    "22763a78d819642815e2ed6b1d3d0944f01f5415c59f7e0955cf5a734a2c522a",
    "c05382c3683b3e0390ca0bdeb64182fadd80af33a2056b314645abb06c3a49b0",
    "e2adb5d6dfde86e715742427644fd9956b4e1308daf6a42864e7edc5e96d6cf7",
  ],
  [
    "examples/big-label",
    "b6640bee549149d8eaf9f9f8e7cddf486d6b3bd50cee2ddbb06a855e1980aa04",
    "d986c626f864b1dd9e3e6a642819200d76f0e67d63da84553f95ceaa2e2ef207",
    "9fdc3f8384f00aa3006abfbbb84e86a7d53fc37d91e2ab1fff854eab4e8fdf63",
  ],
  [
    "examples/jamazon-warehouse",
    "7ce960ad34dc4f77c92e07727acc3113ccf803934671a1fe2f46cf736d2641ce",
    "36b03f261e65be21093aa294715b621b753dbc531e003f1487436c1946f174f9",
    "1dd4dba482714145d1548c414a1320d9567147c28cb0ad57fb37f679bd9408c7",
  ],
  [
    "examples/jamazon/apps/nextjs-betterauth",
    "5806d5c6e98c33ae41c6ba5b3da6cca0ca72d69467521c5381c1babe56adf6b0",
    "f7318f0be5fcbf2065324e4f545c96323edfa4e007186fb89eabff519ca370d8",
    "4f5f563ace252df5e88732c436c8612cf32cec80a1631d5c936d95cb781dd474",
  ],
  [
    "examples/music-agent/apps/nextjs",
    "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
    "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
    "34bad58a3a9312662fc1753ed4f4ba21dae1e4d655ae0991bda5efa885a081d6",
  ],
  [
    "examples/music-agent/apps/ts-hono",
    "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
    "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
    "34bad58a3a9312662fc1753ed4f4ba21dae1e4d655ae0991bda5efa885a081d6",
  ],
  [
    "examples/poster-shop/apps/nextjs-betterauth",
    "55a9ac8b000f23198f161d0e64ea96e10fc608de6c8edabad0925d39c472badc",
    "c2c80e4c3ecf667af8dd57d937fd72d5b6198e85d36753987ba3047838fb9fd9",
    "ac35ceadb34f5cf3bf20cb7b438534381f17cb3e79d441f37aa31244c405b6d1",
  ],
  [
    "examples/record-player/apps/next-betterauth",
    "0f726114893031ae7b5326c91ab91869f9713d0d54b9a9e753957080ee66370c",
    "fa66c6bd2a11cdf22cc6efb1e5c7737070b322de45163e636068309507671292",
    "e562c31280e66aff0ba160178a5e682e5688b3fe773bf68a91a109e459f5ed2a",
  ],
  [
    "examples/stage-plan/apps/sveltekit-hybrid",
    "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
    "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
    "e68899aa31aa0a7d380050fc17cd78ee6b7fca5b19aed3de952e0b8813e881b5",
  ],
  [
    "examples/stage-plan/apps/ts-effect-betterauth",
    "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
    "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
    "e68899aa31aa0a7d380050fc17cd78ee6b7fca5b19aed3de952e0b8813e881b5",
  ],
  [
    "examples/wequencer/apps/next-betterauth",
    "a0d57d731cd5c19c2810769738e14ba99c2b0cd2665e4b11e44d3e73d4502b67",
    "c1f2524cc44f9ca8d099559c30f3b51e230d846119cfe3e92ced05962a7fa6b3",
    "24fc7fa5efb0818190a4aa7d3300ec38f65ef81f4958ed749d37c802f3e20a34",
  ],
] as const;

type Compiled = { current: string } & ClientMigrations;

function compiled(appFile: string | object): Compiled {
  return compileTestApp(appFile as never).schema as unknown as Compiled;
}

function versionOf(bundle: Compiled, hash: string): WasmSchema {
  const version = bundle.schemas.find((v) => v.hash === hash);
  if (!version) throw new Error(`no version ${hash}`);
  return (version.schema as { tables: WasmSchema }).tables;
}

const issuerDrop = [
  {
    table: "better_auth_account",
    added: false,
    removed: false,
    renamedFrom: null,
    operations: [
      {
        type: "drop",
        column: "issuer",
        column_type: { type: "Text" },
        value: { type: "Text", value: "" },
      },
    ],
  },
];

describe("shipped Better Auth issuer-removal histories", () => {
  it.each(examples)(
    "keeps the exact connected upgrade for %s",
    (directory, fromHash, toHash, historyDigest) => {
      const bundle = compiled(readFileSync(join(root, directory, "jazz.app.json"), "utf8"));
      expect(bundle.current).toBe(toHash);
      expect(bundle.schemas.map((v) => v.hash).sort()).toEqual([fromHash, toHash].sort());
      const fromVersion = bundle.schemas.find((v) => v.hash === fromHash)!;
      expect(createHash("sha256").update(JSON.stringify(fromVersion.schema)).digest("hex")).toBe(
        historyDigest,
      );
      expect(bundle.migrations).toEqual([{ fromHash, toHash, forward: issuerDrop }]);
      const from = versionOf(bundle, fromHash);
      const to = versionOf(bundle, toHash);
      const { better_auth_account: account, ...unchanged } = from;
      const strip = (schema: WasmSchema) =>
        Object.fromEntries(
          Object.entries(schema).map(([name, { relations: _, ...table }]) => [name, table]),
        );
      // History versions carry no relations; compare what both versions store.
      expect(strip(to)).toEqual(
        strip({
          ...unchanged,
          better_auth_account: {
            ...account!,
            columns: account!.columns.filter((column) => column.name !== "issuer"),
          },
        }),
      );
      // Password/provider/account/user coordinates remain in the migrated view.
      for (const name of ["password", "providerId", "accountId", "userId"]) {
        expect(to.better_auth_account!.columns.find((column) => column.name === name)).toEqual(
          account!.columns.find((column) => column.name === name),
        );
      }
    },
  );

  it("compiles the frozen real example definitions to the shipped upgrade endpoints", () => {
    expect(compiled(legacyAppFile).current).toBe(examples[0][1]);
    expect(compiled(currentAppFile).current).toBe(examples[0][2]);
    expect([legacyHash, currentHash]).toEqual([examples[0][1], examples[0][2]]);
  });

  it("refuses the structural issuer removal without a migration lens", () => {
    const file = JSON.parse(currentAppFile);
    file.migrations = [{ from: "8acd8a85eef5" }];
    expect(() => compiled(file)).toThrow(
      /does not produce the columns of table "better_auth_account" \("issuer" is left over\)/,
    );
  });

  it("refuses attaching the old hash to a witness with its issuer already removed", () => {
    // In the file a version is named, not hashed: a history version that is
    // already the issuer-less schema can't stand in for the issuer-era one.
    const file = JSON.parse(currentAppFile);
    delete file.history["8acd8a85eef5"].tables.better_auth_account.columns.issuer;
    expect(() => compiled(file)).toThrow(/history\.8acd8a85eef5 is the current schema/);
  });
});
