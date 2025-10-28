import { useAccount } from "jazz-tools/react";

function App() {
  const { logOut } = useAccount();

  return (
    <main>
      <h1>You're logged in</h1>
      <button onClick={() => logOut()}>Logout</button>
    </main>
  );
}

export default App;
