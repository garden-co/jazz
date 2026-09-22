import { JazzProvider, useJazzClient } from "jazz-tools/react";
import { Suspense, useEffect, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { prepareAccountConfig } from "./account";

import { Loader2Icon } from "lucide-react";
import { CreateChatRedirect } from "@/components/CreateChatRedirect";
import { ChatList } from "@/components/chat-list/ChatList";
import { ChatView } from "@/components/chat-view/ChatView";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { InviteHandler } from "@/components/InviteHandler";
import { NavBar } from "@/components/navbar/NavBar";
import Router from "@/components/Router";
import { RouterScope } from "@/hooks/useRouter";

interface AppProps {
  config?: Partial<DbConfig>;
  initialPath?: string;
}

export function App({ config, initialPath }: AppProps = {}) {
  const app = <AppInner config={config} />;
  return initialPath === undefined ? (
    app
  ) : (
    <RouterScope initialPath={initialPath}>{app}</RouterScope>
  );
}

function AppInner({ config }: { config?: Partial<DbConfig> }) {
  const [resolved, setResolved] = useState<DbConfig>();
  const [error, setError] = useState<Error>();
  useEffect(() => {
    let cancelled = false;
    prepareAccountConfig(config).then(
      (value) => {
        if (!cancelled) setResolved(value);
      },
      (cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
      },
    );
    return () => {
      cancelled = true;
    };
  }, [config]);
  if (error) throw error;
  if (!resolved) return <p id="joining-chat">Loading...</p>;

  return (
    <JazzProvider config={resolved} fallback={<p id="joining-chat">Loading...</p>}>
      <ExposeDevClient />
      <AppContent />
    </JazzProvider>
  );
}

function ExposeDevClient() {
  const client = useJazzClient();

  useEffect(() => {
    if (!["localhost", "127.0.0.1"].includes(window.location.hostname)) return;
    (window as unknown as { jazzClient?: typeof client }).jazzClient = client;
  }, [client]);

  return null;
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
