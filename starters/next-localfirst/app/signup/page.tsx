import { notFound } from "next/navigation";
import { SignUpForm } from "./sign-up-form";

export default function SignUpPage() {
  if (process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH !== "1") {
    notFound();
  }

  return <SignUpForm />;
}
