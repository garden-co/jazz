import { useIsAuthenticated } from "jazz-tools/expo";
import { type ReactNode, useState } from "react";
import { SignInScreen } from "./SignInScreen";
import { SignUpScreen } from "./SignUpScreen";

export function AuthScreen({ children }: { children: ReactNode }) {
  const isAuthenticated = useIsAuthenticated();
  const [page, setPage] = useState<"sign-in" | "sign-up">("sign-in");

  if (isAuthenticated) {
    return children;
  }

  if (page === "sign-in") {
    return <SignInScreen setPage={setPage} />;
  }

  return <SignUpScreen setPage={setPage} />;
}
