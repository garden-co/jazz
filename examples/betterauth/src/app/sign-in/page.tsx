"use client";

import { SignInForm } from "jazz-react-auth-betterauth";

export default function SignInPage() {
  return <SignInForm.default providers={["github"]} />;
}
