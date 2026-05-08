/* eslint-disable */

const { execSync } = require("node:child_process");
const { readFileSync } = require("node:fs");

const { version: packageVersion } = require("./package.json");

const binaryName = "jazz-napi";
const packageName = "@garden-co/jazz-napi";
const loadErrors = [];

function load(specifier) {
  try {
    return require(specifier);
  } catch (error) {
    loadErrors.push(error);
    return null;
  }
}

function loadPackage(specifier) {
  try {
    const binding = require(specifier);
    const bindingPackageVersion = require(`${specifier}/package.json`).version;
    if (
      bindingPackageVersion !== packageVersion &&
      process.env.NAPI_RS_ENFORCE_VERSION_CHECK &&
      process.env.NAPI_RS_ENFORCE_VERSION_CHECK !== "0"
    ) {
      throw new Error(
        `Native binding package version mismatch, expected ${packageVersion} but got ${bindingPackageVersion}. You can reinstall dependencies to fix this issue.`,
      );
    }
    return binding;
  } catch (error) {
    loadErrors.push(error);
    return null;
  }
}

function isMusl() {
  if (process.platform !== "linux") {
    return false;
  }
  try {
    return readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {
    // Fall through to runtime report / ldd command probes.
  }
  const report =
    typeof process.report?.getReport === "function" ? process.report.getReport() : null;
  if (report?.header?.glibcVersionRuntime) {
    return false;
  }
  if (Array.isArray(report?.sharedObjects)) {
    return report.sharedObjects.some(
      (entry) => entry.includes("libc.musl-") || entry.includes("ld-musl-"),
    );
  }
  try {
    return execSync("ldd --version", { encoding: "utf8" }).includes("musl");
  } catch {
    return false;
  }
}

function candidatesForPlatform() {
  switch (process.platform) {
    case "darwin":
      return ["darwin-universal", `darwin-${process.arch}`];
    case "win32":
      if (process.arch === "x64") {
        const gnu =
          process.config?.variables?.shlib_suffix === "dll.a" ||
          process.config?.variables?.node_target_type === "shared_library";
        return [gnu ? "win32-x64-gnu" : "win32-x64-msvc"];
      }
      return [`win32-${process.arch}-msvc`];
    case "linux":
      if (process.arch === "x64") {
        return [isMusl() ? "linux-x64-musl" : "linux-x64-gnu"];
      }
      if (process.arch === "arm64") {
        return [isMusl() ? "linux-arm64-musl" : "linux-arm64-gnu"];
      }
      return [`linux-${process.arch}-gnu`];
    case "freebsd":
      return [`freebsd-${process.arch}`];
    default:
      return [];
  }
}

function requireNative() {
  if (process.env.NAPI_RS_NATIVE_LIBRARY_PATH) {
    const binding = load(process.env.NAPI_RS_NATIVE_LIBRARY_PATH);
    if (binding) {
      return binding;
    }
  }

  for (const candidate of candidatesForPlatform()) {
    const local = load(`./${binaryName}.${candidate}.node`);
    if (local) {
      return local;
    }
    const packaged = loadPackage(`${packageName}-${candidate}`);
    if (packaged) {
      return packaged;
    }
  }

  if (process.env.NAPI_RS_FORCE_WASI) {
    const wasi =
      load(`./${binaryName}.wasi.cjs`) || loadPackage(`${packageName}-wasm32-wasi`);
    if (wasi) {
      return wasi;
    }
  }

  throw new Error(
    `Cannot find native ${binaryName} binding for ${process.platform}/${process.arch}`,
    {
      cause: loadErrors[loadErrors.length - 1],
    },
  );
}

const nativeBinding = requireNative();

module.exports = nativeBinding;
module.exports.DevServer = nativeBinding.DevServer;
module.exports.NapiRuntime = nativeBinding.NapiRuntime;
module.exports.TestingServer = nativeBinding.TestingServer;
module.exports.currentTimestamp = nativeBinding.currentTimestamp;
module.exports.deriveUserId = nativeBinding.deriveUserId;
module.exports.generateId = nativeBinding.generateId;
module.exports.getPublicKeyBase64url = nativeBinding.getPublicKeyBase64url;
module.exports.mintLocalFirstToken = nativeBinding.mintLocalFirstToken;
module.exports.parseSchema = nativeBinding.parseSchema;
module.exports.verifyLocalFirstIdentityProof = nativeBinding.verifyLocalFirstIdentityProof;
