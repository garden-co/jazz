import { JazzProvider } from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region branch-react
export function BranchApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        env: "prod",
        userBranch: "staging",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion branch-react
