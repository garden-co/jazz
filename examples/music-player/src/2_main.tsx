import { Toaster } from "@/components/ui/toaster";
import { JazzInspector } from "jazz-tools/inspector";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { HomePage } from "./3_HomePage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import { WelcomeScreen } from "./components/WelcomeScreen";
import "./index.css";

import { MusicaAccount } from "@/1_schema";
import { apiKey } from "@/apiKey.ts";
import { SidebarProvider } from "@/components/ui/sidebar";
import { JazzReactProvider } from "jazz-tools/react";
import { onAnonymousAccountDiscarded } from "./4_actions";
import { KeyboardListener } from "./components/PlayerControls";
import { usePrepareAppState } from "./lib/usePrepareAppState";
import {
  AccountProvider,
  useAccountSelector,
} from "@/components/AccountProvider.tsx";

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
function AppContent({
  mediaPlayer,
}: {
  mediaPlayer: ReturnType<typeof useMediaPlayer>;
}) {
  const showWelcomeScreen = useAccountSelector({
    select: (me) => !me.root.accountSetupCompleted,
  });

  const isReady = usePrepareAppState(mediaPlayer);

  // Show welcome screen if account setup is not completed
  if (showWelcomeScreen) {
    return <WelcomeScreen />;
  }

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

  if (!isReady) return null;

  return (
    <>
      <RouterProvider router={router} />
      <KeyboardListener mediaPlayer={mediaPlayer} />
      <Toaster />
    </>
  );
}

function Main() {
  const mediaPlayer = useMediaPlayer();

  return (
    <SidebarProvider>
      <AppContent mediaPlayer={mediaPlayer} />
      <JazzInspector />
    </SidebarProvider>
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
        <AccountProvider>
          <Main />
        </AccountProvider>
        <JazzInspector />
      </SidebarProvider>
    </JazzReactProvider>
  </React.StrictMode>,
);
