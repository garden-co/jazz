import ReactDOM from "react-dom/client";
import {
  RouterProvider,
  createHashRouter,
  useNavigate,
} from "react-router-dom";
import "./index.css";

import {
  JazzProvider,
  PasskeyAuthBasicUI,
  useAcceptInvite,
  useAccount,
  usePasskeyAuth,
} from "jazz-react";

import React from "react";
import { TodoAccount, TodoProject } from "./1_schema.ts";
import { NewProjectForm } from "./3_NewProjectForm.tsx";
import { ProjectTodoTable } from "./4_ProjectTodoTable.tsx";
import {
  Button,
  ThemeProvider,
  TitleAndLogo,
} from "./basicComponents/index.ts";

/**
 * Walkthrough: The top-level provider `<JazzProvider/>`
 *
 * This shows how to use the top-level provider `<JazzProvider/>`,
 * which provides the rest of the app with a controlled account (used through `useAccount` later).
 * Here we use `PasskeyAuth`, which uses Passkeys (aka WebAuthn) to store a user's account secret
 * - no backend needed.
 *
 * `<JazzProvider/>` also runs our account migration
 */

const appName = "Jazz Todo List Example";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [passkeyAuth, passKeyState] = usePasskeyAuth({ appName });

  return (
    <>
      <JazzProvider
        AccountSchema={TodoAccount}
        auth={passkeyAuth}
        peer="wss://cloud.jazz.tools/?key=todo-example-jazz@garden.co"
      >
        {children}
      </JazzProvider>
      <PasskeyAuthBasicUI state={passKeyState} />
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: TodoAccount;
  }
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider>
      <TitleAndLogo name={appName} />
      <div className="flex flex-col h-full items-center justify-start gap-10 pt-10 pb-10 px-5">
        <JazzAndAuth>
          <App />
        </JazzAndAuth>
      </div>
    </ThemeProvider>
    ,
  </React.StrictMode>,
);

/**
 * Routing in `<App/>`
 *
 * <App> is the main app component, handling client-side routing based
 * on the CoValue ID (CoID) of our TodoProject, stored in the URL hash
 * - which can also contain invite links.
 */
export default function App() {
  // logOut logs out the AuthProvider passed to `<JazzProvider/>` above.
  const { logOut } = useAccount();

  const router = createHashRouter([
    {
      path: "/",
      element: <HomeScreen />,
    },
    {
      path: "/project/:projectId",
      element: <ProjectTodoTable />,
    },
    {
      path: "/invite/*",
      element: <p>Accepting invite...</p>,
    },
  ]);

  // `useAcceptInvite()` is a hook that accepts an invite link from the URL hash,
  // and on success calls our callback where we navigate to the project that we were just invited to.
  useAcceptInvite({
    invitedObjectSchema: TodoProject,
    forValueHint: "project",
    onAccept: (projectID) => router.navigate("/project/" + projectID),
  });

  return (
    <>
      <RouterProvider router={router} />

      <Button
        onClick={() => router.navigate("/").then(logOut)}
        variant="outline"
      >
        Log out
      </Button>
    </>
  );
}

function HomeScreen() {
  const { me } = useAccount({
    root: { projects: [{}] },
  });
  const navigate = useNavigate();

  return (
    <>
      {me?.root.projects.length ? <h1>My Projects</h1> : null}
      {me?.root.projects.map((project) => {
        return (
          <Button
            key={project.id}
            onClick={() => navigate("/project/" + project?.id)}
            variant="ghost"
          >
            {project.title}
          </Button>
        );
      })}
      <NewProjectForm />
    </>
  );
}

/** Walkthrough: Continue with ./3_NewProjectForm.tsx */
