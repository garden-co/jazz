import { Toaster } from "@/components/ui/toaster";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { HomePage } from "./3_HomePage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import { PlayerControls } from "./components/PlayerControls";
import "./index.css";

import { MusicaAccount } from "@/1_schema";
import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";
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
      <PlayerControls mediaPlayer={mediaPlayer} />
      <Toaster />
    </>
  );
}

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = useDemoAuth();

  const peer =
    (new URL(window.location.href).searchParams.get(
      "peer",
    ) as `ws://${string}`) ??
    "wss://cloud.jazz.tools/?key=music-player-example-jazz@garden.co";

  return (
    <>
      <JazzProvider
        storage={["singleTabOPFS", "indexedDB"]}
        auth={auth}
        peer={peer}
        AccountSchema={MusicaAccount}
      >
        {children}
      </JazzProvider>
      <DemoAuthBasicUI appName="Jazz Music Player" state={state} />
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: MusicaAccount;
  }
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzAndAuth>
      <Main />
    </JazzAndAuth>
  </React.StrictMode>,
);
