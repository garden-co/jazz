import { Toaster } from "@/components/ui/toaster";
import { JazzInspector, enableProfiling } from "jazz-tools/inspector";
/* eslint-disable react-refresh/only-export-components */
import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createHashRouter } from "react-router-dom";
import { PlaylistPage } from "./3_PlaylistPage";
import { useMediaPlayer } from "./5_useMediaPlayer";
import { InvitePage } from "./6_InvitePage";
import { SettingsPage } from "./7_SettingsPage";
import { WelcomeScreen } from "./components/WelcomeScreen";
import { ErrorBoundary } from "./components/ErrorBoundary";
import "./index.css";

import { MusicaAccount } from "@/1_schema";
import { apiKey } from "@/apiKey.ts";
import { SidebarProvider } from "@/components/ui/sidebar";
import { JazzReactProvider, useSuspenseAccount } from "jazz-tools/react";
import { onAnonymousAccountDiscarded } from "./4_actions";
import { useSetupAppState } from "./lib/useSetupAppState";

// Normally profiling is enabled only in development mode
// but we enable it for the music player example to show
// profiling data in the production environment
enableProfiling();

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
  const showWelcomeScreen = useSuspenseAccount(MusicaAccount, {
    select: (me) => !me.root.accountSetupCompleted,
  });

  const isReady = useSetupAppState(mediaPlayer);

  // Show welcome screen if account setup is not completed
  if (showWelcomeScreen) {
    return <WelcomeScreen />;
  }

  const router = createHashRouter([
    {
      path: "/",
      element: (
        <ErrorBoundary>
          <PlaylistPage mediaPlayer={mediaPlayer} />
        </ErrorBoundary>
      ),
    },
    {
      path: "/playlist/:playlistId",
      element: (
        <ErrorBoundary>
          <PlaylistPage mediaPlayer={mediaPlayer} />
        </ErrorBoundary>
      ),
    },
    {
      path: "/settings",
      element: (
        <ErrorBoundary>
          <SettingsPage mediaPlayer={mediaPlayer} />
        </ErrorBoundary>
      ),
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
      <Toaster />
    </>
  );
}

function Main() {
  const mediaPlayer = useMediaPlayer();

  return <AppContent mediaPlayer={mediaPlayer} />;
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
      authSecretStorageKey="examples/music-player"
      onAnonymousAccountDiscarded={onAnonymousAccountDiscarded}
    >
      <SidebarProvider>
        <ErrorBoundary>
          <Main />
        </ErrorBoundary>
        <JazzInspector />
      </SidebarProvider>
    </JazzReactProvider>
  </React.StrictMode>,
);
