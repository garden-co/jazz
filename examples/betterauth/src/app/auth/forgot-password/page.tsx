import type { Metadata } from "next";
import { ForgotPasswordForm } from "@/components/forgot-password-form";

export const metadata: Metadata = {
  title: "Forgot password | Jazz Example: Better Auth",
};

export default function ForgotPasswordPage() {
  return <ForgotPasswordForm />;
}
