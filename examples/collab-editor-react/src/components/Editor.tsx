import { useEffect, useState } from "react";
import "../lib/monaco-setup.js";
import MonacoEditor, { type OnMount } from "@monaco-editor/react";
import type * as monaco from "monaco-editor";
import { toast } from "sonner";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app, type Room } from "../../schema.js";
import { useJazzYjsDocument } from "../hooks/useJazzYjsDocument.js";
import { useMonacoBinding } from "../hooks/useMonacoBinding.js";
import { getDisplayName } from "../lib/identity.js";
import { LanguagePicker } from "./LanguagePicker.js";

type EditorProps = {
  shareToken: string;
};

type MonacoRuntime = {
  editor: monaco.editor.IStandaloneCodeEditor;
  monaco: typeof import("monaco-editor");
};

function EditorSurface({ room }: { room: Room }) {
  const db = useDb();
  const { ydoc, isReady } = useJazzYjsDocument({ roomId: room.id });
  const [runtime, setRuntime] = useState<MonacoRuntime | null>(null);
  const [title, setTitle] = useState(room.title);

  useMonacoBinding({
    editor: runtime?.editor ?? null,
    monaco: runtime?.monaco ?? null,
    ydoc,
  });

  useEffect(() => {
    setTitle(room.title);
  }, [room.title]);

  const handleMount: OnMount = (editor, monacoApi) => {
    setRuntime({ editor, monaco: monacoApi });
  };

  const saveTitle = () => {
    const nextTitle = title.trim() || "Untitled";
    setTitle(nextTitle);
    if (nextTitle !== room.title) {
      db.update(app.rooms, room.id, { title: nextTitle });
    }
  };

  const copyLink = async () => {
    await navigator.clipboard.writeText(window.location.href);
    toast.success("Link copied");
  };

  return (
    <main>
      <header>
        <a href="/">Rooms</a>
        <p>you are {getDisplayName()}</p>
        <input
          aria-label="Room title"
          value={title}
          onChange={(event) => setTitle(event.target.value)}
          onBlur={saveTitle}
          onKeyDown={(event) => {
            if (event.key === "Enter") event.currentTarget.blur();
          }}
        />
        <LanguagePicker room={room} />
        <button type="button" onClick={copyLink}>
          Copy link
        </button>
      </header>

      <div data-testid="editor" style={{ height: "70vh", border: "1px solid #ddd" }}>
        {!isReady ? (
          <p>Loading editor...</p>
        ) : (
          <MonacoEditor
            height="70vh"
            language={room.editorLanguage}
            theme="vs-light"
            options={{ minimap: { enabled: false } }}
            onMount={handleMount}
          />
        )}
      </div>
    </main>
  );
}

export function Editor({ shareToken }: EditorProps) {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const rooms = useAll(shareToken ? app.rooms.where({ shareToken }) : undefined);
  const room = rooms?.[0] ?? null;
  const participants = useAll(
    room && sessionUserId
      ? app.roomParticipants.where({ room_id: room.id, session_user_id: sessionUserId })
      : undefined,
  );

  useEffect(() => {
    if (!room || !sessionUserId || !participants) return;

    const now = new Date();
    const participant = participants[0];
    if (participant) {
      db.update(app.roomParticipants, participant.id, { lastAccessedAt: now });
      return;
    }

    db.insert(app.roomParticipants, {
      room_id: room.id,
      session_user_id: sessionUserId,
      lastAccessedAt: now,
    });
  }, [db, participants, room, sessionUserId]);

  if (!rooms) return <p>Loading room...</p>;
  if (!room) return <p>Room not found</p>;

  return <EditorSurface room={room} />;
}
