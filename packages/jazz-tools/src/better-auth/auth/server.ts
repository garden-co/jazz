import {
  AuthContext,
  MiddlewareContext,
  MiddlewareOptions,
  type User,
} from "better-auth";
import { APIError } from "better-auth/api";
import { symmetricDecrypt, symmetricEncrypt } from "better-auth/crypto";
import { BetterAuthPlugin, createAuthMiddleware } from "better-auth/plugins";
import type { Account, AuthCredentials, ID } from "jazz-tools";

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
export const jazzPlugin = () => {
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

  return {
    id: "jazz-plugin",
    schema: {
      user: {
        fields: {
          accountID: {
            type: "string",
            required: true,
          },
          encryptedCredentials: {
            type: "string",
            required: true,
            // returned: false
          },
        },
      },
    },
    hooks: {
      before: [
        {
          matcher: (context) => {
            return context.path.startsWith("/sign-up");
          },
          handler: createAuthMiddleware(async (ctx) => {
            const { body } = ctx;

            if (!body.jazzAuth) {
              throw new APIError(422, {
                message: "JazzAuth is required",
              });
            }

            const { jazzAuth, ...rest } = body;

            const credentials: AuthCredentials = {
              accountID: jazzAuth.accountID as ID<Account>,
              secretSeed: jazzAuth.secretSeed,
              accountSecret: jazzAuth.accountSecret as any,
              provider: jazzAuth.provider,
            };

            const encryptedCredentials = await symmetricEncrypt({
              key: ctx.context.secret,
              data: JSON.stringify(credentials),
            });

            ctx.body = {
              ...rest,
              accountID: jazzAuth.accountID,
              encryptedCredentials: encryptedCredentials,
            };

            return {
              context: ctx,
            };
          }),
        },
      ],
      after: [
        {
          matcher: (context) => {
            return (
              context.path.startsWith("/sign-up") ||
              context.path.startsWith("/sign-in")
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
      ],
    },
  } satisfies BetterAuthPlugin;
};

export interface UserWithJazz extends User {
  encryptedCredentials: string;
  salt: string;
}
