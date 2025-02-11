"use client";

import { Button } from "@/components/ui/button";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
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
      <Button variant="outline" onClick={handleSignOut}>
        Sign out
      </Button>
    );
  }

  return (
    <>
      <Button
        onClick={() => setOpen(true)}
        className="bg-white text-black hover:bg-gray-100"
      >
        Sign up
      </Button>
      <AuthModal open={open} onOpenChange={setOpen} />
    </>
  );
}
