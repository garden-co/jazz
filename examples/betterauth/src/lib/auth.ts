import * as crypto from "node:crypto";
import { appName } from "@/components/emails";
import { betterAuth } from "better-auth";
import { getMigrations } from "better-auth/db";
import { emailOTP, haveIBeenPwned, magicLink } from "better-auth/plugins";
import Database from "better-sqlite3";
import { jazzPlugin } from "jazz-betterauth-server-plugin";
import {
  sendEmailOtpCb,
  sendMagicLinkCb,
  sendResetPasswordCb,
  sendVerificationEmailCb,
  sendWelcomeEmailCb,
} from "./auth-email";
import { socialProviders } from "./socialProviders";

export const auth = await (async () => {
  const generatedKeys = crypto.generateKeyPairSync("ed25519");

  // Configure Better Auth server
  const auth = betterAuth({
    appName: appName,
    secret: undefined as string | undefined,
    database: new Database("sqlite.db"),
    emailAndPassword: {
      enabled: true,
      sendResetPassword: sendResetPasswordCb,
    },
    emailVerification: {
      sendVerificationEmail: sendVerificationEmailCb,
    },
    socialProviders: socialProviders,
    account: {
      accountLinking: {
        allowDifferentEmails: true,
        allowUnlinkingAll: true,
      },
    },
    user: {
      deleteUser: {
        enabled: true,
      },
    },
    plugins: [
      haveIBeenPwned(),
      jazzPlugin(
        generatedKeys.publicKey.export({
          type: "spki",
          format: "pem",
        }) as string,
        process.env.BETTER_AUTH_SECRET,
      ),
      magicLink({
        sendMagicLink: sendMagicLinkCb,
      }),
      emailOTP({ sendVerificationOTP: sendEmailOtpCb }),
    ],
    databaseHooks: {
      user: {
        create: {
          after: sendWelcomeEmailCb,
        },
      },
    },
  });

  // Run database migrations
  const migrations = await getMigrations(auth.options);
  await migrations.runMigrations();

  // Set database encryption secret
  auth.options.secret = process.env.BETTER_AUTH_SECRET;
  if (process.env.BETTER_AUTH_SECRET) {
    const signedSecret = crypto
      .sign(
        undefined,
        Buffer.from(process.env.BETTER_AUTH_SECRET),
        generatedKeys.privateKey,
      )
      .toString("hex");
    auth.api
      .setSecret({
        body: {
          secret: process.env.BETTER_AUTH_SECRET,
          signedSecret,
        },
      })
      .then((response) => response)
      .then((data) => console.log(`Set secret: ${data.status}`))
      .catch((error) => console.error(error));
  }

  return auth;
})();
