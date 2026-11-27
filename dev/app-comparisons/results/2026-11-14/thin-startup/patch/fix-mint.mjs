// Fix 1 (prototype): the thin client's token mint.
// - noble enables an 8-bit window for Ed25519's base point at import, so the
//   first getPublicKey builds a 4096-point table (~40 ms on a fast laptop).
//   A 4-bit window builds 512 points: same results, a few ms per signature.
// - The same token is minted twice per load (restore, then retainSecret):
//   reuse it within the load, and across loads while it has > 10 min left
//   (localStorage, keyed by a hash of issuer + audience + seed).
export function apply({ edit }) {
  edit(
    "thin/self-signed-token.js",
    `const encoder = new TextEncoder();\n`,
    `const encoder = new TextEncoder();
// thin-startup fix 1: a smaller base-point window (noble defaults to 8).
ed25519.Point.BASE.precompute(4);
const mintedTokens = new Map();
const TOKEN_REUSE_MIN_LEFT_S = 600;
function tokenCacheKey(issuer, options) {
    const id = sha512(encoder.encode(\`\${issuer}\\n\${options.audience}\\n\${options.ttlSeconds}\\n\${options.secret}\`)).slice(0, 16);
    return "jazz-thin-token:" + base64url(id);
}
function reusableToken(token, nowSeconds) {
    try {
        const claims = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
        return typeof claims.exp === "number" && claims.exp - Number(nowSeconds) > TOKEN_REUSE_MIN_LEFT_S;
    } catch { return false; }
}
`,
  );
  edit(
    "thin/self-signed-token.js",
    `function mintSelfSignedTokenInner(issuer, options) {`,
    `function mintSelfSignedTokenInner(issuer, options) {
    const key = tokenCacheKey(issuer, options);
    let token = mintedTokens.get(key);
    if (token === undefined) { try { token = globalThis.localStorage?.getItem(key) ?? undefined; } catch {} }
    if (token !== undefined && reusableToken(token, options.nowSeconds)) {
        mintedTokens.set(key, token);
        return token;
    }
    token = mintSelfSignedTokenUncached(issuer, options);
    mintedTokens.set(key, token);
    try { globalThis.localStorage?.setItem(key, token); } catch {}
    return token;
}
function mintSelfSignedTokenUncached(issuer, options) {`,
  );
}
