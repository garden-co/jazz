"use client";

import { useAccount, useIsAuthenticated } from "jazz-tools/react";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { AuthModal } from "./AuthModal";

export function AuthButton() {
  const [open, setOpen] = useState(false);
  const { logOut } = useAccount();
  const navigate = useNavigate();

  const isAuthenticated = useIsAuthenticated();

  function handleSignOut() {
    logOut();
    navigate("/");
  }

  if (isAuthenticated) {
    return (
      <Button variant="ghost" onClick={handleSignOut}>
        Sign out
      </Button>
    );
  }

  return (
    <>
      <Button onClick={() => setOpen(true)} variant="ghost">
        Sign up
      </Button>
      <AuthModal open={open} onOpenChange={setOpen} />
    </>
  );
}
