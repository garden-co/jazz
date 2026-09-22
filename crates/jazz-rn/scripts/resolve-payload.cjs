const { createRequire } = require("node:module");
const { readFileSync, realpathSync, existsSync } = require("node:fs");
const { join, dirname } = require("node:path");

// Resolve from jazz-rn's own dependency graph, not the application's hoisting
// layout. Both targets are dependencies: npm's host OS/CPU is not a mobile ABI.
function resolvePayload(platform, wrapperRoot = join(__dirname, "..")) {
  if (!["android", "ios"].includes(platform)) throw new Error("Unknown RN payload platform");
  const wrapper = JSON.parse(readFileSync(join(wrapperRoot, "package.json"), "utf8"));
  const name = `jazz-rn-${platform}`;
  const requireFromWrapper = createRequire(join(wrapperRoot, "package.json"));
  const manifestPath = requireFromWrapper.resolve(`${name}/package.json`);
  const metadata = JSON.parse(readFileSync(manifestPath, "utf8"));
  const declared = wrapper.dependencies?.[name];
  const previewPrefix = `https://pkg.pr.new/garden-co/jazz/${name}@`;
  const previewCommit =
    typeof declared === "string" && declared.startsWith(previewPrefix)
      ? declared.slice(previewPrefix.length)
      : "";
  const preview = /^[a-f0-9]{40}$/.test(previewCommit);
  if (
    metadata.name !== name ||
    metadata.version !== wrapper.version ||
    (![wrapper.version, "workspace:*"].includes(declared) && !preview)
  )
    throw new Error(`RN payload ${name} must match jazz-rn ${wrapper.version}`);
  if (metadata.os || metadata.cpu || metadata.optionalDependencies || metadata.codegenConfig)
    throw new Error(`RN payload ${name} must be an unconditional data package`);
  const root = realpathSync(dirname(manifestPath));
  if (!existsSync(join(root, platform, "jazz-native-relay.manifest.json")))
    throw new Error(`RN payload ${name} is missing its sealed manifest`);
  if (preview) {
    const manifest = JSON.parse(
      readFileSync(join(root, platform, "jazz-native-relay.manifest.json"), "utf8"),
    );
    if (manifest.sourceRevision !== previewCommit)
      throw new Error("RN preview payload source mismatch");
  }
  return root;
}
module.exports = { resolvePayload };
if (require.main === module) process.stdout.write(resolvePayload(process.argv[2]));
