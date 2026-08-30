import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import test from "node:test";

const packageDir = resolve(import.meta.dirname, "../../crates/jazz-napi");

test("NAPI packing ships one staged host loader and never historical generations", () => {
  const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
    cwd: packageDir,
    encoding: "utf8",
  });
  const jsonStart = output.indexOf("[\n");
  assert.notEqual(jsonStart, -1, `npm pack did not emit its JSON inventory:\n${output}`);
  const [receipt] = JSON.parse(output.slice(jsonStart));
  assert.ok(receipt, "npm pack did not report a package receipt");
  const files = new Set(receipt.files.map((file) => file.path));

  assert.ok(files.has("native-loader.cjs"));
  assert.ok(files.has("native-artifact-fingerprint.cjs"));
  assert.ok([...files].some((file) => /^jazz-napi\.[^.]+\.node$/.test(file)));
  assert.ok([...files].some((file) => /^jazz-napi\.[^.]+\.manifest\.json$/.test(file)));
  assert.ok(
    [...files].every((file) => !file.startsWith(".native-artifacts/")),
    `packed historical native generation: ${[...files].find((file) => file.startsWith(".native-artifacts/"))}`,
  );
  // A platform package contains exactly one selected native binary plus its
  // small JS/declaration surface.  This catches a future broad files glob even
  // if it happens to omit a particular historic directory in a fixture.
  assert.ok(receipt.unpackedSize < 110_000_000, `unexpected unpacked size: ${receipt.unpackedSize}`);
});
