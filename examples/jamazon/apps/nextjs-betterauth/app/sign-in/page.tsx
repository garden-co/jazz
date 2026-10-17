"use client";

import { Suspense } from "react";
import { SignInForm } from "@/src/components/SignInForm";

export default function SignInPage() {
  return (
    <Suspense>
      <SignInForm />
    </Suspense>
  );
}
