import type { betterAuth } from "better-auth";
import type { emailOTP } from "better-auth/plugins";
import type { magicLink } from "better-auth/plugins";

export const sendEmailOtpCb: Parameters<
  typeof emailOTP
>[0]["sendVerificationOTP"] = async ({ email, otp, type }) => {
  const searchParams = new URLSearchParams();
  searchParams.set("email", email);
  searchParams.set("otp", otp);

  let emailLink = "";
  if (type === "sign-in") {
    // Send the OTP for sign-in
    emailLink = `/mockEmail/sign-in?${searchParams.toString()}`;
  } else if (type === "email-verification") {
    // Send the OTP for email verification
    emailLink = `/mockEmail/email-verification?${searchParams.toString()}`;
  } else {
    // Send the OTP for password reset
    emailLink = `/mockEmail/forget-password?${searchParams.toString()}`;
  }
  console.log("Mock email sent: " + emailLink);
};

export const sendMagicLinkCb: Parameters<typeof magicLink>[0]["sendMagicLink"] =
  async ({ email, url }) => {
    const searchParams = new URLSearchParams();
    searchParams.set("url", url);
    const emailLink = `/mockEmail/sign-in?${searchParams.toString()}`;
    console.log("Mock email sent: " + emailLink);
  };

export const sendWelcomeEmailCb: NonNullable<
  NonNullable<
    NonNullable<Parameters<typeof betterAuth>[0]["databaseHooks"]>["user"]
  >["create"]
>["after"] = async (user) => {
  const searchParams = new URLSearchParams();
  searchParams.set("name", user.name);
  const emailLink = `/mockEmail/welcome?${searchParams.toString()}`;
  console.log("Mock email sent: " + emailLink);
};

export const sendVerificationEmailCb: NonNullable<
  Parameters<typeof betterAuth>[0]["emailVerification"]
>["sendVerificationEmail"] = async ({ user, url }) => {
  const searchParams = new URLSearchParams();
  searchParams.set("name", user.name);
  searchParams.set("url", url);
  const emailLink = `/mockEmail/email-verification?${searchParams.toString()}`;
  console.log("Mock email sent: " + emailLink);
};

export const sendResetPasswordCb: NonNullable<
  Parameters<typeof betterAuth>[0]["emailAndPassword"]
>["sendResetPassword"] = async ({ user, url }) => {
  const searchParams = new URLSearchParams();
  searchParams.set("name", user.name);
  searchParams.set("url", url);
  const emailLink = `/mockEmail/forgot-password?${searchParams.toString()}`;
  console.log("Mock email sent: " + emailLink);
};
