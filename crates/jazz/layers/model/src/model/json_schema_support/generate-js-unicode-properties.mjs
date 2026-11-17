// Regenerates `js_unicode_properties.json`: the Unicode property names this
// Node's `RegExp` accepts in `\p{…}` with the `u` flag. Jazz refuses native
// `\p{…}` names outside the table (`regex.rs`), because the browser JSON
// Schema validator could not translate them.
//
//   dir=$(mktemp -d)
//   curl -sfo "$dir/PropertyAliases.txt" https://www.unicode.org/Public/UCD/latest/ucd/PropertyAliases.txt
//   curl -sfo "$dir/PropertyValueAliases.txt" https://www.unicode.org/Public/UCD/latest/ucd/PropertyValueAliases.txt
//   node crates/jazz/layers/model/src/model/json_schema_support/generate-js-unicode-properties.mjs "$dir"
//   pnpm exec oxfmt crates/jazz/layers/model/src/model/json_schema_support/js_unicode_properties.json
//
// Run it with the oldest Node the project supports, so the table holds only
// names every supported engine knows. Note the Node and Unicode versions in
// the table's "//" entry.
import { readFileSync, writeFileSync } from "node:fs";

const ucd = process.argv[2];
if (!ucd)
  throw new Error("usage: generate-js-unicode-properties.mjs <dir with the UCD alias files>");

const accepts = (name) => {
  try {
    new RegExp(`\\p{${name}}`, "u");
    return true;
  } catch {
    return false;
  }
};
const rows = (file) =>
  readFileSync(`${ucd}/${file}`, "utf8")
    .split("\n")
    .filter((line) => line && !line.startsWith("#"))
    .map((line) =>
      line
        .split("#")[0]
        .split(";")
        .map((field) => field.trim()),
    );

const nonBinary = new Set(["General_Category", "gc", "Script", "sc", "Script_Extensions", "scx"]);
// ECMA-262 adds these three to the UCD's binary properties.
const binary = new Set(["Any", "ASCII", "Assigned"]);
for (const names of rows("PropertyAliases.txt")) {
  for (const name of names) if (name && !nonBinary.has(name) && accepts(name)) binary.add(name);
}
const generalCategory = new Set();
const script = new Set();
for (const [property, ...values] of rows("PropertyValueAliases.txt")) {
  for (const value of values) {
    if (!value) continue;
    if (property === "gc" && accepts(`General_Category=${value}`)) generalCategory.add(value);
    if (property === "sc" && accepts(`Script=${value}`)) script.add(value);
  }
}

const table = new URL("./js_unicode_properties.json", import.meta.url);
const comment = JSON.parse(readFileSync(table, "utf8"))["//"];
writeFileSync(
  table,
  `${JSON.stringify({
    "//": comment,
    binary: [...binary].sort(),
    generalCategory: [...generalCategory].sort(),
    script: [...script].sort(),
  })}\n`,
);
