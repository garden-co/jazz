import React from "react";
import { createHashRouter, Navigate, RouterProvider } from "react-router-dom";
import VaultPage from "./pages/vault";
import { useAcceptInvite } from "./main";
import { Folder } from "./schema";

const App: React.FC = () => {
  const router = createHashRouter([
    {
      path: "/",
      element: <Navigate to={"/vault"} />,
    },
    {
      path: "/vault",
      element: <VaultPage />,
    },
    {
      path: "/vault/:sharedFolderId",
      element: <VaultPage />,
    },
    {
      path: "/invite/*",
      element: <p>Accepting invite...</p>,
    },
  ]);

  useAcceptInvite({
    invitedObjectSchema: Folder,
    onAccept: async (sharedFolderId) => {
      router.navigate(`/vault/${sharedFolderId}`);
    },
  });

  return <RouterProvider router={router} />;
};

export default App;
