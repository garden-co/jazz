import * as React from "react";
import { type User, AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import {
  JazzSessionProvider,
  useJazzSessionOwner,
  useAuthProvider,
  useAuthState,
  type JazzSession,
  type JazzClient,
} from "jazz-tools/react";
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
  const { session, error, retry } = useJazzSessionOwner({ appId, serverUrl });
  if (error)
    return (
      <section>
        <p role="alert">{error.message}</p>
        <button onClick={() => void retry().catch(() => {})}>Retry</button>
      </section>
    );
  if (!session) return <p>Preparing Jazz...</p>;
  return <WorkOSSession session={session} />;
}
function WorkOSSession({ session }: { session: JazzSession<JazzClient> }) {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();
  const auth = useAuthProvider(
    session,
    { key: user?.id ?? null, isPending: isLoading },
    {
      getToken: () => getAccessToken({ forceRefresh: true }),
    },
  );
  const fallback = auth.error ? (
    <section>
      <p role="alert">{auth.error.message}</p>
      <button onClick={() => void auth.retry().catch(() => {})}>Retry</button>
    </section>
  ) : auth.isPending ? (
    <p>Resolving account...</p>
  ) : (
    <button onClick={() => void signIn()}>Sign in with WorkOS</button>
  );
  return (
    <JazzSessionProvider session={session} fallback={fallback}>
      {auth.ready && user ? (
        <ChatShell
          user={user}
          onSignIn={signIn}
          onSignOut={() =>
            auth.logout(() => signOut({ returnTo: window.location.href })).catch(() => {})
          }
        />
      ) : (
        fallback
      )}
    </JazzSessionProvider>
  );
}

export function App() {
  return (
    <AuthKitProvider clientId={WORKOS_CLIENT_ID} devMode={true}>
      <JazzApp />
    </AuthKitProvider>
  );
}
