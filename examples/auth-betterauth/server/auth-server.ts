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
import { toNodeHandler } from "better-auth/node";
import { jwt } from "better-auth/plugins";
import Database from "better-sqlite3";
import cors from "cors";
import express from "express";

// Configure BetterAuth with SQLite database
export const auth = betterAuth({
  database: new Database("auth.db"),
  // Trust the React client origin
  trustedOrigins: ["http://localhost:5173"],
  emailAndPassword: {
    enabled: true,
  },
  session: {
    // Sessions last 7 days
    expiresIn: 60 * 60 * 24 * 7,
    // Refresh when 1 day remaining
    updateAge: 60 * 60 * 24,
  },
  plugins: [
    jwt({
      // Custom payload for Jazz policy evaluation
      jwt: {
        definePayload: async ({ user }) => {
          // User type from BetterAuth may have additional fields
          const userRecord = user as Record<string, unknown>;
          return {
            sub: user.id,
            email: user.email,
            name: user.name,
            // Custom claims for Jazz policies
            subscriptionTier: (userRecord.subscriptionTier as string) || "free",
            // Roles array for CONTAINS checks
            roles: (userRecord.roles as string[]) || ["member"],
          };
        },
      },
    }),
  ],
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

// Mount BetterAuth routes using the Node.js adapter
// Important: Don't use express.json() before this handler
app.all("/api/auth/*", toNodeHandler(auth));

// Use express.json() only for non-BetterAuth routes
app.use(express.json());

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
