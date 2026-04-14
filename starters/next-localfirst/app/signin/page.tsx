import { notFound } from "next/navigation";
import { SignInForm } from "./sign-in-form";

export default function SignInPage() {
  if (process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH !== "1") {
    notFound();
  }

  return <SignInForm />;
}
