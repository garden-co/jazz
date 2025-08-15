"use client";

import { Button } from "@/components/ui/button";
import { useIsAuthenticated } from "jazz-tools/react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import { betterAuthClient } from "@/lib/auth-client";
import { SSOButton } from "./SSOButton";
import { ssoProviders } from "../app/auth/sso-providers";

export function UserSettings() {
  const router = useRouter();
  const isAuthenticated = useIsAuthenticated();

  if (!isAuthenticated) {
    return (
      <div className="flex flex-col gap-8">
        <h1 className="text-center text-2xl">Forbidden</h1>
        <p className="text-center text-sm text-muted-foreground">
          Please{" "}
          <Link href="/auth/sign-in" className="underline text-primary">
            sign in
          </Link>{" "}
          to access this page.
        </p>
      </div>
    );
  }

  return (
    <>
      <div className="flex flex-col gap-8">
        <h1 className="text-4xl font-semibold">Settings</h1>

        {ssoProviders.map((provider) => (
          <SSOButton key={provider} provider={provider} link />
        ))}

        <Button
          variant="destructive"
          className="relative"
          type="button"
          onClick={async (e) => {
            e.preventDefault();
            betterAuthClient
              .deleteUser()
              .then(() => router.push("/"))
              .catch((error) =>
                toast.error("Error", {
                  description: error.error.message,
                }),
              );
          }}
        >
          Delete account
        </Button>
      </div>
    </>
  );
}
