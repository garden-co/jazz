import { SignInButton, SignOutButton } from "@clerk/clerk-react";
import { Account } from "jazz-tools";
import { useAccount, useIsAuthenticated } from "jazz-tools/react";

function App() {
  const me = useAccount(Account, { resolve: { profile: true } });

  const isAuthenticated = useIsAuthenticated();

  if (isAuthenticated && me.$isLoaded) {
    return (
      <div className="container">
        <h1>You're logged in</h1>
        <p>Welcome back, {me.profile.name}</p>
        <SignOutButton>Logout</SignOutButton>
      </div>
    );
  }

  return (
    <div className="container">
      <h1>You're not logged in</h1>
      <SignInButton />
    </div>
  );
}

export default App;
