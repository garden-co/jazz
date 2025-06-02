import { useAccount, useIsAuthenticated } from "jazz-react";
import { useJazzWorkOSAuth } from "jazz-react-auth-workos";

function App() {
  const { signIn, signOut, isReady } = useJazzWorkOSAuth()
  const { me } = useAccount();

  const isAuthenticated = useIsAuthenticated();

  const signOutHandler = () => {
    signOut();
  }

  const signInHandler = async () => {
    await signIn()
  }

  if (!isReady) return null

  if (isAuthenticated) {
    return (
      <div className="container">
        <h1>You're logged in</h1>
        <p>Welcome back, {me?.profile?.name}</p>
        <button onClick={signOutHandler}>Sign out</button>
      </div>
    );
  }

  return (
    <div className="container">
      <h1>You're not logged in</h1>
      <button onClick={signInHandler}>
        Sign in
      </button>
    </div>
  );
}

export default App;
