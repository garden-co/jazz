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
  // Configure Better Auth server
  const auth = betterAuth({
    appName: appName,
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
      jazzPlugin(),
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

  return auth;
})();
