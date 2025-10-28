import { SigninForm } from "@/components/signin-form";
import type { Metadata } from "next";
import { ssoProviders } from "../sso-providers";

export const metadata: Metadata = {
  title: "Sign in | Jazz Example: Better Auth",
};

export default function LoginPage() {
  return <SigninForm providers={ssoProviders} />;
}
