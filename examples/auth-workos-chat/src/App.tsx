import * as React from "react";
import { JazzProvider, backendAuth, useJazzAuth, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, APP_ID, CHAT_ID } from "../constants.js";
import { type WorkOSUser, useWorkOSSession } from "./session.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";

type ChatShellProps = {
  user: WorkOSUser | null;
  onSignIn: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
};

function ChatShell({ user, onSignIn, onSignOut }: ChatShellProps) {
  const { claims } = useAuthState();
  const canPostAnnouncements = claims.role === "admin";
  const canPostGeneric = claims.role === "member" || claims.role === "admin";
  const displayName = user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous";
  const statusDetail = user ? "Signed in with WorkOS" : "Sign in with WorkOS to unlock chat-01";

  return (
    <main className="app-shell">
      <section className="content-grid">
        <AuthCard
          role={claims.role as string}
          statusDetail={statusDetail}
          user={user}
          onSignIn={onSignIn}
          onSignOut={onSignOut}
        />

        <ChatPanel
          chatId={ANNOUNCEMENTS_CHAT_ID}
          title="Announcements"
          canSend={canPostAnnouncements}
          authorName={displayName}
          readOnlyNotice="Only admins can post announcements."
        />

        <ChatPanel
          chatId={CHAT_ID}
          title={CHAT_ID}
          canSend={canPostGeneric}
          authorName={displayName}
          readOnlyNotice="Sign in as admin or member to participate."
        />
      </section>
    </main>
  );
}

export function App() {
  const { isLoading, user, signIn, signOut } = useWorkOSSession();
  return (
    // Jazz runs in the backend (server/server.ts) under /jazz; its
    // `authenticate` reads the AuthKit session cookie.
    <JazzProvider
      appId={APP_ID}
      serverUrl="/jazz"
      auth={backendAuth({
        key: user?.email ?? null,
        isPending: isLoading,
        logout: signOut,
      })}
      signedOut={<button onClick={() => void signIn()}>Sign in with WorkOS</button>}
    >
      <SignedInChat user={user} onSignIn={signIn} />
    </JazzProvider>
  );
}

function SignedInChat({ user, onSignIn }: Pick<ChatShellProps, "user" | "onSignIn">) {
  const { logout } = useJazzAuth();
  return <ChatShell user={user} onSignIn={onSignIn} onSignOut={logout} />;
}
