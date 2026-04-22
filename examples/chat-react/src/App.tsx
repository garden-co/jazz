import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";
import { Suspense } from "react";
import type { DbConfig } from "jazz-tools";

import { Loader2Icon } from "lucide-react";
import { CreateChatRedirect } from "@/components/CreateChatRedirect";
import { ChatList } from "@/components/chat-list/ChatList";
import { ChatView } from "@/components/chat-view/ChatView";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { InviteHandler } from "@/components/InviteHandler";
import { NavBar } from "@/components/navbar/NavBar";
import Router from "@/components/Router";

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    secret,
    ...overrides,
  };
}

export function App({ config }: { config?: Partial<DbConfig> } = {}) {
  return <AppInner config={config} />;
}

function AppInner({ config }: { config?: Partial<DbConfig> }) {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <p id="joining-chat">Loading...</p>;
  }

  return (
    <JazzProvider
      config={defaultConfig(secret, config)}
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
