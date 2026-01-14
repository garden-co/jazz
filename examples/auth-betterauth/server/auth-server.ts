/**
 * BetterAuth server for the Jazz auth integration demo.
 *
 * This server provides:
 * - User registration and login
 * - JWT token generation with custom claims
 * - Organization support for multi-tenant access control
 * - JWKS endpoint for token verification
 */

import { betterAuth } from "better-auth";
import Database from "better-sqlite3";
import cors from "cors";
import express from "express";

// Initialize SQLite database for BetterAuth
const db = new Database("auth.db");

// Configure BetterAuth
export const auth = betterAuth({
  database: {
    type: "sqlite",
    // BetterAuth will use the db instance directly
    db,
  },
  emailAndPassword: {
    enabled: true,
  },
  session: {
    // Sessions last 7 days
    expiresIn: 60 * 60 * 24 * 7,
    // Refresh when 1 day remaining
    updateAge: 60 * 60 * 24,
  },
  // JWT configuration for token-based auth with Jazz
  jwt: {
    // Custom payload for Jazz policy evaluation
    definePayload: async ({ user, session }) => {
      return {
        sub: user.id,
        email: user.email,
        name: user.name,
        // Custom claims for Jazz policies
        subscriptionTier: (user as any).subscriptionTier || "free",
        // Organization ID if in an org context
        orgId: (session as any).activeOrganizationId || null,
        // Roles array for CONTAINS checks
        roles: (user as any).roles || ["member"],
      };
    },
  },
});

// Create Express app
const app = express();

// Enable CORS for the React client
app.use(
  cors({
    origin: "http://localhost:5173",
    credentials: true,
  }),
);

app.use(express.json());

// Mount BetterAuth routes
app.all("/api/auth/*", (req, res, _next) => {
  // BetterAuth handles all /api/auth/* routes
  return auth.handler(req, res);
});

// Health check
app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

// Start server
const PORT = process.env.AUTH_PORT || 3001;
app.listen(PORT, () => {
  console.log(`BetterAuth server running on http://localhost:${PORT}`);
  console.log(`JWKS endpoint: http://localhost:${PORT}/api/auth/jwks`);
  console.log(`Login: POST http://localhost:${PORT}/api/auth/sign-in/email`);
  console.log(`Register: POST http://localhost:${PORT}/api/auth/sign-up/email`);
});
