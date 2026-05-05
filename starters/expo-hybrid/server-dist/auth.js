"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.auth = void 0;
const better_auth_1 = require("better-auth");
const memory_1 = require("better-auth/adapters/memory");
const plugins_1 = require("better-auth/plugins");
const api_1 = require("better-auth/api");
const jwt_payload_js_1 = require("./jwt-payload.js");
const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3001";
if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}
const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;
// TODO: Replace with a persistent adapter before shipping.
const authMemoryDb = {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
};
exports.auth = (0, better_auth_1.betterAuth)({
  baseURL: APP_ORIGIN,
  secret: BETTER_AUTH_SECRET,
  database: (0, memory_1.memoryAdapter)(authMemoryDb),
  trustedOrigins: (request) => {
    const origin = request?.headers.get("origin");
    if (origin && new URL(origin).hostname === "localhost") return [origin];
    return [APP_ORIGIN];
  },
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  hooks: {
    before: (0, api_1.createAuthMiddleware)(async (ctx) => {
      if (ctx.path !== "/sign-up/email") return;
      const { verifyLocalFirstIdentityProof } = await import("jazz-napi");
      const {
        ok,
        error,
        id: provedUserId,
      } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "expo-hybrid-signup");
      if (!ok) {
        throw new api_1.APIError("BAD_REQUEST", { message: error });
      }
      return {
        context: {
          ...ctx,
          body: { ...ctx.body, provedUserId },
        },
      };
    }),
  },
  databaseHooks: {
    user: {
      create: {
        before: async (user, context) => {
          const provedUserId = context?.body?.provedUserId;
          if (provedUserId) {
            return { data: { ...user, id: provedUserId } };
          }
        },
      },
    },
  },
  plugins: [
    (0, plugins_1.bearer)(),
    (0, plugins_1.jwt)({
      jwks: {
        keyPairConfig: { alg: "ES256" },
      },
      jwt: {
        expirationTime: "1h",
        issuer: APP_ORIGIN,
        getSubject: ({ user }) => user.id,
        definePayload: jwt_payload_js_1.jwtPayload,
      },
    }),
  ],
});
