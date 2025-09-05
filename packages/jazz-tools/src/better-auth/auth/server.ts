import { AuthContext, MiddlewareContext, MiddlewareOptions } from "better-auth";
import { APIError } from "better-auth/api";
import { symmetricDecrypt, symmetricEncrypt } from "better-auth/crypto";
import { BetterAuthPlugin, createAuthMiddleware } from "better-auth/plugins";
import type { Account, AuthCredentials, ID } from "jazz-tools";

// Define a type to have user fields mapped in the better-auth instance
// It should be automatic, but it needs an hard reference to BetterAuthPlugin type
// in order to be exported as library.
type JazzPlugin = BetterAuthPlugin & {
  schema: {
    user: {
      fields: {
        accountID: {
          type: "string";
          required: false;
          input: false;
        };
        encryptedCredentials: {
          type: "string";
          required: false;
          input: false;
          returned: false;
        };
      };
    };
  };
};

/**
 * @returns The BetterAuth server plugin.
 *
 * @example
 * ```ts
 * const auth = betterAuth({
 *   plugins: [jazzPlugin()],
 *   // ... other BetterAuth options
 * });
 * ```
 */
export const jazzPlugin: () => JazzPlugin = () => {
  return {
    id: "jazz-plugin",
    schema: {
      user: {
        fields: {
          accountID: {
            type: "string",
            required: false,
            input: false,
          },
          encryptedCredentials: {
            type: "string",
            required: false,
            input: false,
            returned: false,
          },
        },
      },
    },

    init() {
      return {
        options: {
          databaseHooks: {
            user: {
              create: {
                before: async (user, context) => {
                  // If the user is created without a jazzAuth, it will throw an error.
                  if (!contextContainsJazzAuth(context)) {
                    throw new APIError(422, {
                      message: "JazzAuth is required on user creation",
                    });
                  }
                  // Decorate the user with the jazz's credentials.
                  return {
                    data: {
                      accountID: context.jazzAuth.accountID,
                      encryptedCredentials:
                        context.jazzAuth.encryptedCredentials,
                    },
                  };
                },
              },
            },
            verification: {
              create: {
                after: async (verification, context) => {
                  /**
                   * For: Email OTP plugin
                   * After a verification is created, if it is from the EmailOTP plugin,
                   * create a new verification value with the jazzAuth with the same expiration.
                   */
                  if (
                    contextContainsJazzAuth(context) &&
                    verification.identifier.startsWith("sign-in-otp-")
                  ) {
                    await context.context.internalAdapter.createVerificationValue(
                      {
                        value: JSON.stringify({ jazzAuth: context.jazzAuth }),
                        identifier: `${verification.identifier}-jazz-auth`,
                        expiresAt: verification.expiresAt,
                      },
                    );
                  }
                },
              },
            },
          },
        },
      };
    },

    hooks: {
      before: [
        /**
         * If the client sends a x-jazz-auth header,
         * we encrypt the credentials and inject them into the context.
         */
        {
          matcher: (context) => {
            return !!context.headers?.get("x-jazz-auth");
          },
          handler: createAuthMiddleware(async (ctx) => {
            const jazzAuth = JSON.parse(ctx.headers?.get("x-jazz-auth")!);

            const credentials: AuthCredentials = {
              accountID: jazzAuth.accountID as ID<Account>,
              secretSeed: jazzAuth.secretSeed,
              accountSecret: jazzAuth.accountSecret as any,
              // If the provider remains 'anonymous', Jazz will not consider us authenticated later.
              provider: "better-auth",
            };

            const encryptedCredentials = await symmetricEncrypt({
              key: ctx.context.secret,
              data: JSON.stringify(credentials),
            });

            return {
              context: {
                ...ctx,
                jazzAuth: {
                  accountID: jazzAuth.accountID,
                  encryptedCredentials: encryptedCredentials,
                },
              },
            };
          }),
        },

        /**
         * For: Social / OAuth2 plugin
         * /callback is the endpoint that BetterAuth uses to authenticate the user coming from a social provider.
         * 1. Catch the state
         * 2. Find the verification value
         * 3. If the verification value contains a jazzAuth, inject into the context to have it in case of registration.
         */
        {
          matcher: (context) => {
            return (
              context.path.startsWith("/callback") ||
              context.path.startsWith("/oauth2/callback")
            );
          },
          handler: createAuthMiddleware(async (ctx) => {
            const state = ctx.query?.state || ctx.body?.state;

            const identifier = `${state}-jazz-auth`;

            const data =
              await ctx.context.internalAdapter.findVerificationValue(
                identifier,
              );

            // if not found, the social plugin will throw later anyway
            if (!data) {
              throw new APIError(404, {
                message: "Verification not found",
              });
            }

            const parsed = JSON.parse(data.value);

            if (parsed && "jazzAuth" in parsed) {
              return {
                context: {
                  ...ctx,
                  jazzAuth: parsed.jazzAuth,
                },
              };
            } else {
              throw new APIError(404, {
                message: "JazzAuth not found in verification value",
              });
            }
          }),
        },
        /**
         * For: Email OTP plugin
         * When the user sends an OTP, we try to find the jazzAuth.
         * If it isn't a sign-up, we expect to not find a verification value.
         */
        {
          matcher: (context) => {
            return context.path.startsWith("/sign-in/email-otp");
          },
          handler: createAuthMiddleware(async (ctx) => {
            const email = ctx.body.email;
            const identifier = `sign-in-otp-${email}-jazz-auth`;

            const data =
              await ctx.context.internalAdapter.findVerificationValue(
                identifier,
              );

            // if not found, it isn't a sign-up
            if (!data || data.expiresAt < new Date()) {
              return;
            }

            const parsed = JSON.parse(data.value);

            if (parsed && "jazzAuth" in parsed) {
              return {
                context: {
                  ...ctx,
                  jazzAuth: parsed.jazzAuth,
                },
              };
            } else {
              throw new APIError(500, {
                message: "JazzAuth not found in verification value",
              });
            }
          }),
        },
      ],
      after: [
        /**
         * This middleware is used to extract the jazzAuth from the user and return it in the response.
         * It is used in the following endpoints that return the user:
         * - /sign-up/email
         * - /sign-in/email
         * - /get-session
         */
        {
          matcher: (context) => {
            return (
              context.path.startsWith("/sign-up") ||
              context.path.startsWith("/sign-in") ||
              context.path.startsWith("/get-session")
            );
          },
          handler: createAuthMiddleware({}, async (ctx) => {
            const returned = ctx.context.returned as any;
            if (!returned?.user?.id) {
              return;
            }
            const jazzAuth = await extractJazzAuth(returned.user.id, ctx);

            return ctx.json({
              ...returned,
              jazzAuth: jazzAuth,
            });
          }),
        },

        /**
         * For: Social / OAuth2 plugin
         * When the user sign-in via social, we create a verification value with the jazzAuth.
         */
        {
          matcher: (context) => {
            return context.path.startsWith("/sign-in/social");
          },
          handler: createAuthMiddleware(async (ctx) => {
            if (!contextContainsJazzAuth(ctx)) {
              throw new APIError(500, {
                message: "JazzAuth not found in context",
              });
            }

            const returned = ctx.context.returned as { url: string };

            const url = new URL(returned.url);
            const state = url.searchParams.get("state");

            const value = JSON.stringify({ jazzAuth: ctx.jazzAuth });
            const expiresAt = new Date();
            expiresAt.setMinutes(expiresAt.getMinutes() + 10);

            await ctx.context.internalAdapter.createVerificationValue({
              value,
              identifier: `${state}-jazz-auth`,
              expiresAt,
            });
          }),
        },
      ],
    },
  } satisfies JazzPlugin;
};

function contextContainsJazzAuth(ctx: unknown): ctx is {
  jazzAuth: {
    accountID: string;
    encryptedCredentials: string;
  };
} {
  return !!ctx && typeof ctx === "object" && "jazzAuth" in ctx;
}

async function extractJazzAuth(
  userId: string,
  ctx: MiddlewareContext<
    MiddlewareOptions,
    AuthContext & {
      returned?: unknown;
      responseHeaders?: Headers;
    }
  >,
) {
  const user = await ctx.context.adapter.findOne<{
    accountID: string;
    encryptedCredentials: string;
  }>({
    model: ctx.context.tables.user!.modelName,
    where: [
      {
        field: "id",
        operator: "eq",
        value: userId,
      },
    ],
    select: ["accountID", "encryptedCredentials"],
  });

  if (!user) {
    return;
  }

  const jazzAuth = JSON.parse(
    await symmetricDecrypt({
      key: ctx.context.secret,
      data: user.encryptedCredentials,
    }),
  );

  return jazzAuth;
}
