import express from "express";
import type { Request, Response } from "express";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { createServer, type Server as HttpServer } from "node:http";
import { AUTH_JWT_KID } from "../constants.js";
import { findUser, createUser } from "./users.js";

export interface AuthServerOptions {
  port?: number;
  jwtKid?: string;
  issuer?: string;
}

export interface AuthServerHandle {
  port: number;
  url: string;
  stop: () => Promise<void>;
}

const JWA_ALGORITHM = "ES256";

export async function startAuthServer(options: AuthServerOptions = {}): Promise<AuthServerHandle> {
  const port = options.port ?? 3001;
  const issuer = options.issuer ?? `http://127.0.0.1:${port}`;
  const kid = options.jwtKid ?? AUTH_JWT_KID;

  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    console.log(`[auth-server] ${req.method} ${req.url}`);
    next();
  });

  app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "ok" });
  });

  // 1. Generate a fresh ES256 key pair on startup.
  // In production, load the private key from a secure secret store instead.
  const { privateKey, publicKey } = await generateKeyPair(JWA_ALGORITHM);
  const publicJwk = await exportJWK(publicKey);

  // 2. Expose the public key as a JWKS endpoint.
  app.get("/.well-known/jwks.json", (_req: Request, res: Response) => {
    res.json({
      keys: [{ ...publicJwk, kid, alg: JWA_ALGORITHM }],
    });
  });

  async function issueToken(userId: string, username: string, role: string): Promise<string> {
    return new SignJWT({ username, claims: { role } })
      .setProtectedHeader({ alg: JWA_ALGORITHM, kid })
      .setSubject(userId)
      .setIssuer(issuer)
      .setIssuedAt()
      .sign(privateKey);
  }

  // 3a. Sign in — returns a JWT for an existing account.
  app.post("/api/auth/sign-in", async (req: Request, res: Response) => {
    const { email, password } = req.body as { email?: string; password?: string };

    if (!email?.trim() || !password) {
      res.status(400).json({ error: "Email and password are required." });
      return;
    }

    const user = findUser(email.trim(), password);
    if (!user) {
      res.status(401).json({ error: "Invalid email or password." });
      return;
    }

    const token = await issueToken(user.userId, user.username, user.role);
    res.json({ token, username: user.username });
  });

  // 3b. Sign up — creates a new account and returns a JWT.
  app.post("/api/auth/sign-up", async (req: Request, res: Response) => {
    const { email, password } = req.body as { email?: string; password?: string };

    if (!email?.trim() || !password) {
      res.status(400).json({ error: "Email and password are required." });
      return;
    }

    const user = createUser(email.trim(), password);
    if (!user) {
      res.status(409).json({ error: "An account with this email already exists." });
      return;
    }

    const token = await issueToken(user.userId, user.username, user.role);
    res.json({ token, username: user.username });
  });

  const server: HttpServer = createServer(app);
  await new Promise<void>((resolve, reject) => {
    server.listen(port, "127.0.0.1", (error?: unknown) => {
      if (error) reject(error);
      else resolve();
    });
  });

  return {
    port,
    url: issuer,
    stop: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      }),
  };
}
