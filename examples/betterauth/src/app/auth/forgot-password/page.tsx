import { ForgotPasswordForm } from "@/components/forgot-password-form";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Forgot password | Jazz Example: Better Auth",
};

export default function ForgotPasswordPage() {
  return <ForgotPasswordForm />;
}
