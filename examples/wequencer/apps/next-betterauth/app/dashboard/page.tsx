"use client";

import { authClient } from "@/lib/auth-client";
import { SessionBrowser } from "@/components/session-browser";

export default function DashboardPage() {
  const { data: session } = authClient.useSession();
  if (!session) return null;

  async function handleSignOut() {
    await authClient.signOut();
    window.location.assign("/");
  }

  return (
    <main className="dashboard">
      <header className="sequencer-header">
        <div>
          <p className="eyebrow">JAZZ EXAMPLE</p>
          <h1>Wequencer</h1>
        </div>
        <div className="auth-nav">
          <p>Hello, {session.user.name}</p>
          <p data-testid="member-id">Your member ID: {session.user.id}</p>
          <button type="button" onClick={handleSignOut}>
            Sign out
          </button>
        </div>
      </header>
      <SessionBrowser userId={session.user.id} displayName={session.user.name} />
    </main>
  );
}
