import * as crypto from "node:crypto";
import type { BetterAuthPlugin } from "better-auth";
import {
  APIError,
  createAuthEndpoint,
  sessionMiddleware,
} from "better-auth/api";
import type { AgentSecret } from "cojson";
import type { Account, AuthCredentials, ID } from "jazz-tools";
import { z } from "zod";
import { passwordDecrypt, passwordEncrypt } from "./crypto.js";
import type { UserWithJazz } from "./types.js";

const encrypt = async (
  credentials: AuthCredentials,
  passwordSecret: string,
) => {
  const credentialsString = JSON.stringify(credentials);
  return await passwordEncrypt(credentialsString, passwordSecret);
};

const decrypt = async (
  encryptedCredentials: string,
  passwordSecret: string,
  salt: string,
): Promise<AuthCredentials> => {
  const credentialsString = await passwordDecrypt(
    encryptedCredentials,
    passwordSecret,
    salt,
  );
  return JSON.parse(credentialsString);
};

let symmetricSecret: string | undefined = undefined;

export const jazzPlugin = (publicKey: string) => {
  return {
    id: "jazz-plugin",
    endpoints: {
      setSecret: createAuthEndpoint(
        "/jazz-plugin/set-secret",
        {
          method: "POST",
          body: z.object({
            secret: z.string(),
            signedSecret: z.string(),
          }),
          metadata: {
            openapi: {
              summary: "Set the symmetric encryption secret",
              description:
                "Sets the secret used for encryption & decryption of Jazz authentication credentials.",
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
          const verified = crypto.verify(
            undefined,
            Buffer.from(ctx.body.secret),
            publicKey,
            Buffer.from(ctx.body.signedSecret, "hex"),
          );
          if (verified) {
            const oldSecret = symmetricSecret;
            // If the old secret exists, update all users' encrypted credentials
            if (oldSecret && symmetricSecret) {
              const users = await ctx.context.adapter.findMany<UserWithJazz>({
                model: "user",
              });
              for (const user of users) {
                const [encryptedCredentials, salt] = await encrypt(
                  await decrypt(
                    user.encryptedCredentials,
                    oldSecret,
                    user.salt,
                  ),
                  symmetricSecret,
                );
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
                    salt: salt,
                  },
                });
              }
            }
            symmetricSecret = ctx.body.secret;
            return ctx.json({ status: true });
          } else {
            throw new APIError("BAD_REQUEST", {
              message: "Could not verify secret signature",
            });
          }
        },
      ),
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
          if (!symmetricSecret) {
            throw new APIError("BAD_REQUEST", {
              message: "Symmetric secret is not set",
            });
          }
          const credentials: AuthCredentials = {
            accountID: ctx.body.accountID as ID<Account>,
            secretSeed: ctx.body.secretSeed
              ? Uint8Array.from(ctx.body.secretSeed)
              : undefined,
            accountSecret: ctx.body.accountSecret as AgentSecret,
            provider: ctx.body.provider,
          };
          const [encryptedCredentials, salt] = await encrypt(
            credentials,
            symmetricSecret,
          );
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
              salt: salt,
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
          if (!symmetricSecret) {
            throw new APIError("BAD_REQUEST", {
              message: "Symmetric secret is not set",
            });
          }
          return ctx.json(
            await decrypt(
              user.encryptedCredentials,
              symmetricSecret,
              user.salt,
            ),
          );
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
          salt: {
            type: "string",
            required: false,
          },
        },
      },
    },
  } satisfies BetterAuthPlugin;
};

export * from "./types.js";
