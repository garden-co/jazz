import { SignInButton, SignOutButton } from "@clerk/clerk-react";
import { useAccount, useIsAuthenticated } from "jazz-react";

function App() {
  const { me } = useAccount();

  const isAuthenticated = useIsAuthenticated();

  if (isAuthenticated) {
    return (
      <div className="container">
        <h1>You're logged in</h1>
        <p>Welcome back, {me?.profile?.name}</p>
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
