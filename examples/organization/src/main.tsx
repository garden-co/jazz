import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { AcceptInvitePage } from "./AcceptInvitePage.tsx";
import { HomePage } from "./HomePage.tsx";
import { OrganizationPage } from "./OrganizationPage.tsx";
import { JazzAccount } from "./schema.ts";

function Router() {
  const router = createHashRouter([
    {
      path: "/",
      element: <HomePage />,
    },
    {
      path: "/organizations/:organizationId",
      element: <OrganizationPage />,
    },
    {
      path: "/invite/*",
      element: <AcceptInvitePage />,
    },
  ]);

  return <RouterProvider router={router}></RouterProvider>;
}

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, authState] = useDemoAuth();

  return (
    <>
      <JazzProvider
        AccountSchema={JazzAccount}
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=organization-example@garden.co"
      >
        {children}
      </JazzProvider>

      {authState.state !== "signedIn" && (
        <DemoAuthBasicUI appName="Organization" state={authState} />
      )}
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: JazzAccount;
  }
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzAndAuth>
      <Router />
    </JazzAndAuth>
  </StrictMode>,
);
