// Fix 1a only (for a first visit, or a token older than 50 min): the smaller
// Ed25519 base-point window, without the token cache.
export function apply({ edit }) {
  edit("thin/self-signed-token.js", `const encoder = new TextEncoder();\n`, `const encoder = new TextEncoder();\ned25519.Point.BASE.precompute(4);\n`);
}
