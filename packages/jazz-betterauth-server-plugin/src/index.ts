import type { BetterAuthPlugin } from "better-auth";
import { createAuthEndpoint, sessionMiddleware } from "better-auth/api";
import { symmetricDecrypt, symmetricEncrypt } from "better-auth/crypto";
import type { AgentSecret } from "cojson";
import type { Account, AuthCredentials, ID } from "jazz-tools";
import { z } from "zod";
import type { UserWithJazz } from "./types.js";

export const jazzPlugin = () => {
  return {
    id: "jazz-plugin",
    endpoints: {
      encryptCredentials: createAuthEndpoint(
        "/jazz-plugin/encrypt-credentials",
        {
          method: "POST",
          use: [sessionMiddleware],
          body: z.object({
            accountID: z.string(),
            secretSeed: z.number().min(0).max(255).array().optional(),
            accountSecret: z.string(),
            provider: z.string().optional(),
          }),
          metadata: {
            openapi: {
              summary: "Encrypt Jazz authentication credentials",
              description: "Encrypts Jazz authentication credentials.",
              responses: {
                200: {
                  description: "Successful response",
                  content: {
                    "application/json": {
                      schema: {
                        type: "object",
                        properties: {
                          status: {
                            type: "boolean",
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        async (ctx) => {
          const user = ctx.context.session.user as UserWithJazz;
          const credentials: AuthCredentials = {
            accountID: ctx.body.accountID as ID<Account>,
            secretSeed: ctx.body.secretSeed
              ? Uint8Array.from(ctx.body.secretSeed)
              : undefined,
            accountSecret: ctx.body.accountSecret as AgentSecret,
            provider: ctx.body.provider,
          };
          const encryptedCredentials = await symmetricEncrypt({
            key: ctx.context.secret,
            data: JSON.stringify(credentials),
          });
          await ctx.context.adapter.update<UserWithJazz>({
            model: "user",
            where: [
              {
                field: "id",
                value: user.id,
              },
            ],
            update: {
              encryptedCredentials: encryptedCredentials,
            },
          });
          return ctx.json({ status: true });
        },
      ),
      decryptCredentials: createAuthEndpoint(
        "/jazz-plugin/decrypt-credentials",
        {
          method: "GET",
          use: [sessionMiddleware],
          metadata: {
            openapi: {
              summary: "Decrypt Jazz authentication credentials",
              description:
                "Decrypts the encrypted Jazz authentication credentials.",
              responses: {
                200: {
                  description: "Successful response",
                  content: {
                    "application/json": {
                      schema: {
                        type: "object",
                        properties: {
                          accountID: {
                            type: "string",
                          },
                          secretSeed: {
                            type: "array",
                            items: {
                              type: "integer",
                              minimum: 0,
                              maximum: 255,
                            },
                          },
                          accountSecret: {
                            type: "string",
                          },
                          provider: {
                            type: "string",
                          },
                          providerID: {
                            type: "string",
                          },
                        },
                        required: ["accountID", "accountSecret"],
                      },
                    },
                  },
                },
              },
            },
          },
        },
        async (ctx) => {
          const user = ctx.context.session.user as UserWithJazz;
          const decrypted: AuthCredentials = JSON.parse(
            await symmetricDecrypt({
              key: ctx.context.secret,
              data: user.encryptedCredentials,
            }),
          );
          return ctx.json(decrypted);
        },
      ),
    },
    schema: {
      user: {
        fields: {
          encryptedCredentials: {
            type: "string",
            required: false,
          },
        },
      },
    },
  } satisfies BetterAuthPlugin;
};

export * from "./types.js";
