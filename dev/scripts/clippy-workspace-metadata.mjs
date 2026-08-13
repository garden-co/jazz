// Read Cargo's already-computed metadata from stdin and print the manifest
// paths of actual workspace members. Keeping JSON parsing here avoids trying
// to duplicate Cargo's workspace/glob/exclude semantics in POSIX shell.
import { readFileSync } from "node:fs";

const metadata = JSON.parse(readFileSync(0, "utf8"));
const members = new Set(metadata.workspace_members);
for (const pkg of metadata.packages) {
  if (members.has(pkg.id)) console.log(pkg.manifest_path);
}
