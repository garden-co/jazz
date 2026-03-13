const fs = require("node:fs");
const path = require("node:path");

const checkOnly = process.argv.includes("--check");
const loaderPath = path.join(__dirname, "..", "index.js");
const packageJsonPath = path.join(__dirname, "..", "package.json");
const packageVersion = JSON.parse(fs.readFileSync(packageJsonPath, "utf8")).version;

const helperBlock = [
  "const NATIVE_BINDING_PACKAGE_SCOPE = '@garden-co'",
  "const resolveNativeBindingPackage = (name) => `${NATIVE_BINDING_PACKAGE_SCOPE}/${name}`",
  "const requireNativeBindingPackage = (name) => require(resolveNativeBindingPackage(name))",
  "const getNativeBindingPackageVersion = (name) =>",
  "  require(`${resolveNativeBindingPackage(name)}/package.json`).version",
  "",
].join("\n");

const unscopedNativeRequirePattern = /require\((['"])(jazz-napi-[^'"/)]+)\1\)/g;
const nativePackageVersionPattern =
  /require\((['"])(jazz-napi-[^'"/)]+)\/package\.json\1\)\.version/g;

function updateLoader(source) {
  let next = source;

  if (!next.includes("const NATIVE_BINDING_PACKAGE_SCOPE")) {
    const anchorPattern = /const loadErrors = \[\];?\n/;
    const anchorMatch = next.match(anchorPattern);
    if (!anchorMatch) {
      throw new Error(`Could not find loadErrors anchor in ${loaderPath}`);
    }

    next = next.replace(anchorPattern, `${anchorMatch[0]}${helperBlock}`);
  }

  next = next.replace(
    nativePackageVersionPattern,
    (_match, quote, packageName) =>
      `getNativeBindingPackageVersion(${quote}${packageName}${quote})`,
  );

  next = next.replace(
    unscopedNativeRequirePattern,
    (_match, quote, packageName) => `requireNativeBindingPackage(${quote}${packageName}${quote})`,
  );

  next = next.replace(
    /bindingPackageVersion !== ['"][^'"]+['"]/g,
    `bindingPackageVersion !== '${packageVersion}'`,
  );

  next = next.replace(
    /expected [^ ]+ but got \$\{bindingPackageVersion\}/g,
    `expected ${packageVersion} but got \${bindingPackageVersion}`,
  );

  return next;
}

function validateLoader(source) {
  const remainingUnscoped = [...source.matchAll(unscopedNativeRequirePattern)].map(
    (match) => match[2],
  );

  if (remainingUnscoped.length > 0) {
    throw new Error(
      `Scoped loader repair left unscoped native package imports: ${[
        ...new Set(remainingUnscoped),
      ].join(", ")}`,
    );
  }

  if (!source.includes("@garden-co")) {
    throw new Error("Scoped loader repair did not inject the @garden-co package scope");
  }

  if (!source.includes(`bindingPackageVersion !== '${packageVersion}'`)) {
    throw new Error(
      `Scoped loader repair did not sync native binding version checks to ${packageVersion}`,
    );
  }
}

const original = fs.readFileSync(loaderPath, "utf8");
const updated = updateLoader(original);

validateLoader(updated);

if (checkOnly) {
  if (updated !== original) {
    throw new Error(
      `Scoped loader drift detected in ${loaderPath}. Run \`node crates/jazz-napi/scripts/ensure-scoped-loader.js\`.`,
    );
  }

  console.log(`Scoped loader is up to date at ${packageVersion}`);
  process.exit(0);
}

if (updated !== original) {
  fs.writeFileSync(loaderPath, updated);
  console.log(`Updated ${loaderPath} for scoped native packages at ${packageVersion}`);
} else {
  console.log(`Scoped loader already up to date at ${packageVersion}`);
}
