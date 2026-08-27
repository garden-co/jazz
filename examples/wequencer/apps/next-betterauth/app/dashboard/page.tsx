"use client";

import { authClient } from "@/lib/auth-client";
import { SessionBrowser } from "@/components/session-browser";

export default function DashboardPage() {
  const { data: session } = authClient.useSession();
  if (!session) return null;
  const issuer = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

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
      <SessionBrowser issuer={issuer} />
    </main>
  );
}
