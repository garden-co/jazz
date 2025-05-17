import { SignupForm } from "@/components/signup-form";
import type { Metadata } from "next";
import { ssoProviders } from "../sso-providers";

export const metadata: Metadata = {
  title: "Sign up | Jazz Example: Better Auth",
};

export default function LoginPage() {
  return <SignupForm providers={ssoProviders} />;
}
