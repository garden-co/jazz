#!/usr/bin/env node
// Checks the registries' trusted publishers for release-publish.yml without
// publishing: release-publish.yml mode=verify-trust runs it in the `publish`
// environment (the same workflow file and environment a publish presents).
//
// - npm: for each package, exchanges the job's OIDC token for a publish token
//   (POST /-/npm/v1/oidc/token/exchange/package/<name>, as `npm publish` does).
//   Success means npm has a trusted publisher for this workflow and
//   environment on that package. The token is discarded unused; it expires on
//   its own (npm has no revoke endpoint for it).
// - crates.io: exchanges the OIDC token for a trusted-publishing token
//   (POST /api/v1/trusted_publishing/tokens) and revokes it at once
//   (DELETE). crates.io scopes the token to every crate whose trusted
//   publisher matches, and doesn't say which: success proves at least one
//   crate matches. Per crate, only a publish (or the crate's settings page)
//   tells.
// Nothing is printed of any token. Exits 1 if any check failed.
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const NPM_PACKAGES = [
  "jazz-wasm",
  "jazz-napi",
  "@garden-co/jazz-napi-linux-x64-gnu",
  "@garden-co/jazz-napi-linux-arm64-gnu",
  "@garden-co/jazz-napi-darwin-x64",
  "@garden-co/jazz-napi-darwin-arm64",
  "@garden-co/jazz-napi-win32-x64-msvc",
  "jazz-rn",
  "jazz-rn-android",
  "jazz-rn-ios",
  "jazz-tools",
  "create-jazz",
];

/** A package name as npm puts it in a registry path (only the scope's slash escaped). */
export function escapedName(name) {
  return name.replace("/", "%2f");
}

async function idToken(audience) {
  const { ACTIONS_ID_TOKEN_REQUEST_URL: url, ACTIONS_ID_TOKEN_REQUEST_TOKEN: bearer } = process.env;
  if (!url || !bearer) throw new Error("no OIDC token available: the job needs id-token: write");
  const response = await fetch(`${url}&audience=${encodeURIComponent(audience)}`, {
    headers: { Authorization: `Bearer ${bearer}` },
  });
  if (!response.ok) throw new Error(`OIDC token for ${audience}: HTTP ${response.status}`);
  return (await response.json()).value;
}

async function checkNpm(rows) {
  const jwt = await idToken("npm:registry.npmjs.org");
  for (const name of NPM_PACKAGES) {
    const response = await fetch(
      `https://registry.npmjs.org/-/npm/v1/oidc/token/exchange/package/${escapedName(name)}`,
      { method: "POST", headers: { Authorization: `Bearer ${jwt}` } },
    );
    let detail = "";
    if (response.ok) {
      const body = await response.json().catch(() => ({}));
      detail = body.token ? "publish token issued (discarded)" : "no token in the answer";
      rows.push({ registry: "npm", name, ok: Boolean(body.token), detail });
    } else {
      detail = (await response.text()).slice(0, 200).replace(/\s+/g, " ");
      rows.push({ registry: "npm", name, ok: false, detail: `HTTP ${response.status} ${detail}` });
    }
  }
}

async function checkCrates(rows) {
  const jwt = await idToken("crates.io");
  const headers = { "Content-Type": "application/json", "User-Agent": "jazz-release-verify-trust" };
  const response = await fetch("https://crates.io/api/v1/trusted_publishing/tokens", {
    method: "POST",
    headers,
    body: JSON.stringify({ jwt }),
  });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 200).replace(/\s+/g, " ");
    rows.push({
      registry: "crates.io",
      name: "(any crate)",
      ok: false,
      detail: `HTTP ${response.status} ${detail}`,
    });
    return;
  }
  const { token } = await response.json();
  const revoke = await fetch("https://crates.io/api/v1/trusted_publishing/tokens", {
    method: "DELETE",
    headers: { ...headers, Authorization: `Bearer ${token}` },
  });
  rows.push({
    registry: "crates.io",
    name: "(at least one crate)",
    ok: true,
    detail: `token issued, revoke: HTTP ${revoke.status}`,
  });
}

async function main() {
  const rows = [];
  for (const check of [checkNpm, checkCrates]) {
    try {
      await check(rows);
    } catch (error) {
      rows.push({
        registry: check === checkNpm ? "npm" : "crates.io",
        name: "-",
        ok: false,
        detail: error.message,
      });
    }
  }
  const lines = [
    "### Trusted publishers for release-publish.yml (environment publish)",
    "",
    "| registry | package | ok | detail |",
    "|---|---|---|---|",
    ...rows.map((r) => `| ${r.registry} | ${r.name} | ${r.ok ? "✅" : "❌"} | ${r.detail} |`),
    "",
  ];
  console.log(lines.join("\n"));
  if (process.env.GITHUB_STEP_SUMMARY)
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${lines.join("\n")}\n`);
  if (rows.some((r) => !r.ok)) process.exit(1);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}
