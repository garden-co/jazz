import fs from "node:fs";
import path from "node:path";

const [commit, ...directories] = process.argv.slice(2);
if (!/^[a-f0-9]{40}$/.test(commit ?? "")) throw new Error("Invalid preview commit");
const packages = Object.create(null);
let cli;
for (const directory of directories) {
  const manifest = JSON.parse(fs.readFileSync(path.join(directory, "package.json"), "utf8"));
  const { name } = manifest;
  if (
    typeof name !== "string" ||
    !/^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/.test(name) ||
    manifest.private ||
    Object.hasOwn(packages, name)
  )
    throw new Error("Invalid or duplicate preview package");
  packages[name] = `https://pkg.pr.new/garden-co/jazz/${name}@${commit}`;
  if (name === "create-jazz") cli = { directory, version: manifest.version };
}
if (!cli || !packages["jazz-tools"])
  throw new Error("Preview must publish create-jazz and jazz-tools together");
fs.writeFileSync(
  path.join(cli.directory, "jazz-source-snapshot.json"),
  JSON.stringify(
    {
      schema: 2,
      packageVersion: cli.version,
      commit,
      packages,
    },
    null,
    2,
  ) + "\n",
);
