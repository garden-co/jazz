import { useAccount, useIsAuthenticated } from "jazz-react";

export function Counter() {
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
      <h2>Count: {me.root.count}</h2>
      <div style={{ display: "flex", gap: "1rem" }}>
        <button
          onClick={() => {
            me.root.count = me?.root.count - 1;
          }}
        >
          Decrement
        </button>

        <button
          onClick={() => {
            me.root.count = me?.root.count + 1;
          }}
        >
          Increment
        </button>
      </div>
    </div>
  );
}
