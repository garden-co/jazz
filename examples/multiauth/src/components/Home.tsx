import { Account } from "jazz-tools";
import { useAccount, useIsAuthenticated, useLogOut } from "jazz-tools/react";

export function Home() {
  const me = useAccount(Account, { resolve: { profile: true } });
  const logOut = useLogOut();
  const isAuthenticated = useIsAuthenticated();

  if (!me.$isLoaded) return;
  if (!isAuthenticated) return;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: "1rem",
      }}
    >
      <div className="max-w-2xl mx-auto">
        <h1>You're logged in</h1>
        <p>Welcome back, {me.profile.name}</p>
        <button onClick={() => logOut()}>Logout</button>
      </div>
    </div>
  );
}
