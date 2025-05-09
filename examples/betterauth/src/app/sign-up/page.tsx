"use client";

import { SignUpForm } from "jazz-react-auth-betterauth";

export default function SignUpPage() {
  return <SignUpForm.default providers={["github"]} />;
}
