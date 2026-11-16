// Local OIDC issuer for the .65 language gate: discovery + JWKS + token mint (RS256).
// GET /.well-known/openid-configuration, /jwks, /token?sub=..&aud=..&kind=valid|expired&claims=<json>
import { createServer } from "node:http";
import { generateKeyPair, exportJWK, SignJWT } from "jose";
const PORT = Number(process.env.OIDC_PORT ?? 8790);
const ISSUER = `http://127.0.0.1:${PORT}`;
const { publicKey, privateKey } = await generateKeyPair("RS256");
const jwk = { ...(await exportJWK(publicKey)), kid: "gate-1", alg: "RS256", use: "sig" };
createServer(async (req, res) => {
  const u = new URL(req.url ?? "/", ISSUER);
  res.setHeader("content-type", "application/json");
  if (u.pathname === "/.well-known/openid-configuration") return res.end(JSON.stringify({ issuer: ISSUER, jwks_uri: `${ISSUER}/jwks`, id_token_signing_alg_values_supported: ["RS256"] }));
  if (u.pathname === "/jwks") return res.end(JSON.stringify({ keys: [jwk] }));
  if (u.pathname === "/token") {
    const now = Math.floor(Date.now() / 1000); const claims = u.searchParams.get("claims") ? JSON.parse(u.searchParams.get("claims")!) : {};
    const t = await new SignJWT(claims).setProtectedHeader({ alg: "RS256", kid: "gate-1" }).setIssuer(ISSUER).setSubject(u.searchParams.get("sub") ?? "alice")
      .setAudience(u.searchParams.get("aud") ?? "jazz:stageplan-gate").setIssuedAt(now - 5).setExpirationTime(u.searchParams.get("kind") === "expired" ? now - 60 : now + 900).sign(privateKey);
    res.setHeader("content-type", "text/plain"); return res.end(t);
  }
  res.statusCode = 404; res.end("{}");
}).listen(PORT, "127.0.0.1", () => console.log(`oidc up ${ISSUER}`));
