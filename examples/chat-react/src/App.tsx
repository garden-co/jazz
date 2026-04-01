import { createJazzClient, getActiveSyntheticAuth, JazzProvider } from "jazz-tools/react";
import { Suspense } from "react";

type DbConfig = Parameters<typeof createJazzClient>[0];

import { Loader2Icon } from "lucide-react";
import { CreateChatRedirect } from "@/components/CreateChatRedirect";
import { ChatList } from "@/components/chat-list/ChatList";
import { ChatView } from "@/components/chat-view/ChatView";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { InviteHandler } from "@/components/InviteHandler";
import { NavBar } from "@/components/navbar/NavBar";
import Router from "@/components/Router";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID || "019d4349-2486-7021-a33e-566b0820c5af";
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL || undefined;
const ADMIN_SECRET = import.meta.env.VITE_JAZZ_ADMIN_SECRET || undefined;

function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? APP_ID;
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  return {
    appId,
    env: "dev",
    userBranch: "main",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    ...(SERVER_URL ? { serverUrl: SERVER_URL } : {}),
    ...(ADMIN_SECRET ? { adminSecret: ADMIN_SECRET } : {}),
    ...overrides,
  };
}

export function App({ config }: { config?: Partial<DbConfig> } = {}) {
  const resolvedConfig = defaultConfig(config);

  return (
    <JazzProvider
      config={resolvedConfig}
      createJazzClient={createJazzClient}
      fallback={<p id="joining-chat">Loading...</p>}
    >
      <AppContent />
    </JazzProvider>
  );
}

function AppContent() {
  return (
    <main className="flex flex-col h-screen bg-muted text-muted-foreground">
      <NavBar />

      <Router
        routes={[
          {
            path: "/",
            component: () => (
              <ErrorBoundary>
                <Suspense
                  fallback={
                    <div className="p-8 text-center text-muted-foreground italic">Loading...</div>
                  }
                >
                  <CreateChatRedirect />
                </Suspense>
              </ErrorBoundary>
            ),
          },
          {
            path: "/chat/:id",
            component: ({ params }) => (
              <ErrorBoundary>
                <Suspense
                  fallback={
                    <div className="flex-1 grid place-items-center p-8 text-center text-muted-foreground italic">
                      <div className="flex gap-2">
                        <Loader2Icon className="animate-spin" />
                        Loading chat...
                      </div>
                    </div>
                  }
                >
                  <ChatView chatId={params?.id || ""} />
                </Suspense>
              </ErrorBoundary>
            ),
          },
          {
            path: "/chats",
            component: () => (
              <ErrorBoundary>
                <Suspense
                  fallback={
                    <div className="p-8 text-center text-muted-foreground italic">
                      Loading your chats...
                    </div>
                  }
                >
                  <ChatList />
                </Suspense>
              </ErrorBoundary>
            ),
          },
          {
            path: "/invite/:chatId/:code",
            component: ({ params }) => (
              <ErrorBoundary>
                <Suspense
                  fallback={
                    <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
                      Joining chat...
                    </div>
                  }
                >
                  <InviteHandler chatId={params?.chatId || ""} code={params?.code || ""} />
                </Suspense>
              </ErrorBoundary>
            ),
          },
        ]}
      />
    </main>
  );
}
