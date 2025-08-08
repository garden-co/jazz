import { JazzInspector } from "jazz-tools/inspector";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { createHashRouter, RouterProvider } from "react-router-dom";
import { Toaster } from "@/components/ui/toaster";
import { HomePage } from "./3_HomePage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import "./index.css";

import { JazzReactProvider } from "jazz-tools/react";
import { MusicaAccount } from "@/1_schema";
import { apiKey } from "@/apiKey.ts";
import { SidebarProvider } from "@/components/ui/sidebar";
import { onAnonymousAccountDiscarded } from "./4_actions";
import { KeyboardListener } from "./components/PlayerControls";
import { useUploadExampleData } from "./lib/useUploadExampleData";

/**
 * Walkthrough: The top-level provider `<JazzReactProvider/>`
 *
 * This shows how to use the top-level provider `<JazzReactProvider/>`,
 * which provides the rest of the app with a controlled account (used through `useAccount` later).
 * Here we use `DemoAuth` which is great for prototyping you app without wasting time on figuring out
 * the best way to do auth.
 *
 * `<JazzReactProvider/>` also runs our account migration
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
    <JazzReactProvider
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
    </JazzReactProvider>
  </React.StrictMode>,
);
