import { Suspense } from "react";
import { assert, reatomRoute, urlAtom } from "@reatom/core";
import { Loader2Icon } from "lucide-react";
import { ChatList } from "@/components/chat-list/ChatList";
import { ChatView } from "@/components/chat-view/ChatView";
import { CreateChatRedirect } from "@/components/CreateChatRedirect";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { InviteHandler } from "@/components/InviteHandler";
import { NavBar } from "@/components/navbar/NavBar";

export const layoutRoute = reatomRoute({
  layout: true,
  render({ outlet }) {
    return (
      <main className="flex flex-col h-screen bg-muted text-muted-foreground">
        <NavBar />
        <ErrorBoundary key={urlAtom().pathname}>{outlet()}</ErrorBoundary>
      </main>
    );
  },
});

export const homeRoute = layoutRoute.reatomRoute({
  path: "",
  render: (route) => (
    <Suspense
      key={route.name}
      fallback={<div className="p-8 text-center text-muted-foreground italic">Loading...</div>}
    >
      <CreateChatRedirect />
    </Suspense>
  ),
});

export const chatsRoute = layoutRoute.reatomRoute({
  path: "chats",
  render: (route) => (
    <Suspense
      key={route.name}
      fallback={
        <div className="p-8 text-center text-muted-foreground italic">Loading your chats...</div>
      }
    >
      <ChatList />
    </Suspense>
  ),
});

export const chatRoute = layoutRoute.reatomRoute({
  path: "chat/:chatId",
  render(route) {
    const params = route();
    assert(params, "chatRoute render called without params");
    return (
      <Suspense
        key={route.name}
        fallback={
          <div className="flex-1 grid place-items-center p-8 text-center text-muted-foreground italic">
            <div className="flex gap-2">
              <Loader2Icon className="animate-spin" />
              Loading chat...
            </div>
          </div>
        }
      >
        <ChatView key={params.chatId} chatId={params.chatId} />
      </Suspense>
    );
  },
});

export const inviteRoute = layoutRoute.reatomRoute({
  path: "invite/:chatId/:code",
  render(route) {
    const params = route();
    assert(params, "inviteRoute render called without params");
    return (
      <Suspense
        key={route.name}
        fallback={
          <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
            Joining chat...
          </div>
        }
      >
        <InviteHandler chatId={params.chatId} code={params.code} />
      </Suspense>
    );
  },
});
