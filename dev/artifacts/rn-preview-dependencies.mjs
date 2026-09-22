import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
const commit = process.argv[2];
if (!/^[a-f0-9]{40}$/.test(commit ?? ""))
  throw new Error("RN preview requires exact source commit");
const root = resolve(process.argv[3] ?? "crates/jazz-rn");
const path = `${root}/package.json`;
const wrapper = JSON.parse(readFileSync(path, "utf8"));
for (const platform of ["android", "ios"]) {
  const name = `jazz-rn-${platform}`;
  const payload = JSON.parse(readFileSync(`${root}/npm/${platform}/package.json`, "utf8"));
  if (payload.name !== name || payload.version !== wrapper.version)
    throw new Error("RN preview payload version mismatch");
  wrapper.dependencies[name] = `https://pkg.pr.new/garden-co/jazz/${name}@${commit}`;
}
writeFileSync(path, JSON.stringify(wrapper, null, 2) + "\n");
