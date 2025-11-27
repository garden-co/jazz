import { useLogOut } from "jazz-tools/react";

function App() {
  const logOut = useLogOut();

  return (
    <main>
      <h1>You're logged in</h1>
      <button onClick={() => logOut()}>Logout</button>
    </main>
  );
}

export default App;
