import { useAccount, useIsAuthenticated } from "jazz-react";

export function Home() {
  const { me, logOut } = useAccount({ root: {} });
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
      <div className="container">
        <h1>You're logged in</h1>
        <p>Welcome back, {me?.profile?.name}</p>
        <button onClick={() => logOut()}>Logout</button>
      </div>
    </div>
  );
}
