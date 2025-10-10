"use client";

import { useAccount, usePasskeyAuth } from "jazz-tools/react";
import { config } from "../config";

export function AuthButton() {
  const { logOut } = useAccount();

  const auth = usePasskeyAuth({
    appName: config.appName,
  });

  function handleLogOut() {
    logOut();
    window.history.pushState({}, "", "/");
    window.location.reload();
  }

  if (auth.state === "signedIn") {
    return (
      <button
        className="w-full min-w-max cursor-pointer bg-zinc-200 hover:bg-zinc-300 transition-all py-1 px-3 text-sm rounded-md"
        onClick={handleLogOut}
      >
        Log out
      </button>
    );
  }

  return (
    <>
      <button
        className="w-full min-w-max cursor-pointer bg-zinc-200 hover:bg-zinc-300 transition-all py-1 px-3 text-sm rounded-md"
        onClick={() => auth.signUp("")}
      >
        Sign up
      </button>
      <button
        onClick={() => auth.logIn()}
        className="w-full min-w-max cursor-pointer bg-zinc-200 hover:bg-zinc-300 transition-all py-1 px-3 text-sm rounded-md"
      >
        Log in
      </button>
    </>
  );
}
