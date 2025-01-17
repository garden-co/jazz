"use client";

import { useAccount, useIsAnonymousUser, usePasskeyAuth } from "jazz-react";
import { APPLICATION_NAME } from "./main";

export function AuthButton() {
  const { logOut } = useAccount();

  const isAnonymousUser = useIsAnonymousUser();

  const [, authState] = usePasskeyAuth({
    appName: APPLICATION_NAME,
    onAnonymousUserUpgrade: ({ isSignUp }) => {
      if (isSignUp) {
        console.log(
          "User signed up using passkeys, the changes done locally are preserved",
        );
      } else {
        console.log(
          "User logged in using passkeys, the changes done locally are lost!",
        );
      }
    },
  });

  function handleLogOut() {
    logOut();
    window.history.pushState({}, "", "/");
  }

  if (!isAnonymousUser) {
    return (
      <button
        className="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
        onClick={handleLogOut}
      >
        Log out
      </button>
    );
  }

  if (authState.state !== "ready") return null;

  return (
    <div className="flex gap-2">
      <button
        className="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
        onClick={() => authState.signUp("")}
      >
        Sign up
      </button>
      <button
        onClick={() => authState.logIn()}
        className="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
      >
        Log in
      </button>
    </div>
  );
}
