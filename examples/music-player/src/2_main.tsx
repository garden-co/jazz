import { Toaster } from "@/components/ui/toaster";
import { JazzInspector } from "jazz-inspector";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { HomePage } from "./3_HomePage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import "./index.css";

import { MusicaAccount } from "@/1_schema";
import { apiKey } from "@/apiKey.ts";
import { SidebarProvider } from "@/components/ui/sidebar";
import { JazzProvider } from "jazz-react";
import { onAnonymousAccountDiscarded } from "./4_actions";
import { KeyboardListener } from "./components/PlayerControls";
import { useUploadExampleData } from "./lib/useUploadExampleData";

/**
 * Walkthrough: The top-level provider `<JazzProvider/>`
 *
 * This shows how to use the top-level provider `<JazzProvider/>`,
 * which provides the rest of the app with a controlled account (used through `useAccount` later).
 * Here we use `DemoAuth` which is great for prototyping you app without wasting time on figuring out
 * the best way to do auth.
 *
 * `<JazzProvider/>` also runs our account migration
 */

function Main() {
  const mediaPlayer = useMediaPlayer();

  useUploadExampleData();

  const router = createHashRouter([
    {
      path: "/",
      element: <HomePage mediaPlayer={mediaPlayer} />,
    },
    {
      path: "/playlist/:playlistId",
      element: <HomePage mediaPlayer={mediaPlayer} />,
    },
    {
      path: "/invite/*",
      element: <InvitePage />,
    },
  ]);

  return (
    <>
      <RouterProvider router={router} />
      <KeyboardListener mediaPlayer={mediaPlayer} />
      <Toaster />
    </>
  );
}

const peer =
  (new URL(window.location.href).searchParams.get(
    "peer",
  ) as `ws://${string}`) ?? `wss://cloud.jazz.tools/?key=${apiKey}`;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzProvider
      sync={{
        peer,
      }}
      storage="indexedDB"
      AccountSchema={MusicaAccount}
      defaultProfileName="Anonymous unicorn"
      onAnonymousAccountDiscarded={onAnonymousAccountDiscarded}
    >
      <SidebarProvider>
        <Main />
        <JazzInspector />
      </SidebarProvider>
    </JazzProvider>
  </React.StrictMode>,
);
