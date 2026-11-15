// Local JWKS + token mint for the native-backend security gate.
// Serves http://localhost:$JWKS_PORT/.well-known/jwks.json and /token?sub=alice&kind=valid|expired|wrong-iss|bad-sig|no-sub
import { createServer } from "node:http";
import { generateKeyPair, exportJWK, SignJWT } from "jose";
const PORT = Number(process.env.JWKS_PORT ?? 8690);
const ISSUER = process.env.ISSUER ?? `http://localhost:${PORT}`;
const AUD = process.env.AUDIENCE; // optional
const { publicKey, privateKey } = await generateKeyPair("RS256");
const other = await generateKeyPair("RS256");
const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "RS256", use: "sig" };
async function mint(sub: string, kind: string, claims: Record<string, unknown>) {
  const now = Math.floor(Date.now() / 1000);
  let j = new SignJWT({ ...claims }).setProtectedHeader({ alg: "RS256", kid: "k1" }).setIssuedAt(now - 10);
  if (kind !== "no-sub") j = j.setSubject(sub);
  j = j.setIssuer(kind === "wrong-iss" ? "https://evil.example.test" : ISSUER);
  if (AUD) j = j.setAudience(AUD);
  j = j.setExpirationTime(kind === "expired" ? now - 60 : now + 3600);
  return j.sign(kind === "bad-sig" ? other.privateKey : privateKey);
}
createServer(async (req, res) => {
  const u = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  if (u.pathname === "/.well-known/jwks.json") { res.setHeader("content-type", "application/json"); return res.end(JSON.stringify({ keys: [jwk] })); }
  if (u.pathname === "/token") {
    const claims = u.searchParams.get("claims") ? JSON.parse(u.searchParams.get("claims")!) : {};
    return res.end(await mint(u.searchParams.get("sub") ?? "alice", u.searchParams.get("kind") ?? "valid", claims));
  }
  res.statusCode = 404; res.end();
}).listen(PORT, () => console.log(`jwks up ${PORT} issuer=${ISSUER}`));
