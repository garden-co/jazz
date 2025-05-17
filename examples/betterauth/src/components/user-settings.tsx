"use client";

import { Button } from "@/components/ui/button";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useAuth } from "jazz-react-auth-betterauth";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { toast } from "sonner";

export function UserSettings() {
  const router = useRouter();
  const { authClient } = useAuth();
  const { logOut } = useAccount();
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

        <Button
          variant="destructive"
          className="relative"
          type="button"
          onClick={async (e) => {
            e.preventDefault();
            authClient.deleteUser(undefined, {
              onSuccess: () => {
                logOut();
                router.push("/");
              },
              onError: (error) => {
                toast.error("Error", {
                  description: error.error.message,
                });
              },
            });
          }}
        >
          Delete account
        </Button>
      </div>
    </>
  );
}
