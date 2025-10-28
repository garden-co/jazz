import { Account } from "jazz-tools";
import { useAccount, useIsAuthenticated } from "jazz-tools/react";

export function Home() {
  const { me, logOut } = useAccount(Account, { resolve: { root: true } });
  const isAuthenticated = useIsAuthenticated();

  if (!me) return;
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
        <p>Welcome back, {me?.profile?.name}</p>
        <button onClick={() => logOut()}>Logout</button>
      </div>
    </div>
  );
}
