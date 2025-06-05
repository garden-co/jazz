import { JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { apiKey } from "@/apiKey.ts";
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

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      AccountSchema={JazzAccount}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <Router />
    </JazzProvider>
  </StrictMode>,
);
