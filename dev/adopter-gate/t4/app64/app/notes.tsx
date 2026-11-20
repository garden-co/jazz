"use client";
import { useState } from "react";
import { useAll, useJazzSession } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { app } from "@/schema";
import { AuthForm } from "./auth-form";

export function Notes() {
  const session = useJazzSession();
  const user = session.user;
  const team = (user?.claims as { team?: string } | undefined)?.team;
  const { data: teamNotes = [], db } = useAll(
    app.team_notes.select("id", "team", "author_name", "text", "sent_at").orderBy("sent_at", "asc"),
  );
  const { data: privateNotes = [] } = useAll(
    app.private_notes.select("id", "text", "sent_at").orderBy("sent_at", "asc"),
  );
  const [text, setText] = useState("");
  const [err, setErr] = useState("");

  async function addPrivate() {
    setErr("");
    try {
      await db.insert(app.private_notes, { text, sent_at: new Date() }).wait({ tier: "global" });
    } catch (e) {
      setErr(String(e));
    }
  }
  async function addTeam() {
    setErr("");
    try {
      await db
        .insert(app.team_notes, {
          team: team ?? "none",
          author_name: user?.identity.subject ?? "?",
          text,
          sent_at: new Date(),
        })
        .wait({ tier: "global" });
    } catch (e) {
      setErr(String(e));
    }
  }
  async function signOut() {
    await authClient.signOut();
    await session.refresh();
  }

  return (
    <div>
      <pre data-testid="whoami">
        {JSON.stringify({
          status: session.status,
          accountId: user?.accountId,
          kind: user?.kind,
          claims: user?.claims ?? null,
        })}
      </pre>
      {user?.kind === "device" ? (
        <AuthForm />
      ) : (
        <button data-testid="sign-out" onClick={() => void signOut()}>
          Sign out
        </button>
      )}
      <div>
        <input data-testid="text" value={text} onChange={(e) => setText(e.target.value)} />
        <button data-testid="add-private" onClick={() => void addPrivate()}>
          Add private note
        </button>
        <button data-testid="add-team" onClick={() => void addTeam()}>
          Add team note
        </button>
        <span data-testid="err">{err}</span>
      </div>
      <h3>Team notes</h3>
      <ul data-testid="team-notes">
        {teamNotes.map((n) => (
          <li key={n.id}>
            {n.team}|{n.text}
          </li>
        ))}
      </ul>
      <h3>Private notes</h3>
      <ul data-testid="private-notes">
        {privateNotes.map((n) => (
          <li key={n.id}>{n.text}</li>
        ))}
      </ul>
    </div>
  );
}
