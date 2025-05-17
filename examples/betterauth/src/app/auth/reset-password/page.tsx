import { ResetPasswordForm } from "@/components/reset-password-form";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Reset password | Jazz Example: Better Auth",
};

export default function ResetPasswordPage() {
  return <ResetPasswordForm />;
}
