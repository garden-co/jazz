"use client";

import { authClient } from "@/lib/auth-client";
import { SessionBrowser } from "@/components/session-browser";
import { useGracefulSignOut } from "@/components/jazz-provider";

export default function DashboardPage() {
  const { data: session } = authClient.useSession();
  const gracefulSignOut = useGracefulSignOut();
  if (!session) return null;

  async function handleSignOut() {
    try {
      await gracefulSignOut();
      window.location.assign("/");
    } catch {
      // The owner-held lifecycle reopens the selected client and renders the error.
    }
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
          <p data-testid="member-id">Your Jazz account loads after enrollment.</p>
          <button type="button" onClick={handleSignOut}>
            Sign out
          </button>
        </div>
      </header>
      <SessionBrowser />
    </main>
  );
}
