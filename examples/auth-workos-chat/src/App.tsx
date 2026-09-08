import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { JazzProvider, jwtAuth, useJazzAuth, useAuthState } from "jazz-tools/react";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID, WORKOS_CLIENT_ID } from "../constants.js";
import { ChatPanel } from "./ChatPanel.js";
import { AuthCard } from "./AuthCard.js";

type ChatShellProps = {
  user: User | null;
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

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function JazzApp() {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();
  return (
    <JazzProvider
      appId={appId}
      serverUrl={serverUrl}
      auth={jwtAuth({
        key: user?.id ?? null,
        isPending: isLoading,
        getToken: () => getAccessToken({ forceRefresh: true }),
        logout: () => signOut({ returnTo: window.location.href }),
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

export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
