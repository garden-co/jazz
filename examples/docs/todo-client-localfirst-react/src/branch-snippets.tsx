import { JazzProvider } from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region branch-react
export function BranchApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
        env: "prod",
        userBranch: "staging",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion branch-react
