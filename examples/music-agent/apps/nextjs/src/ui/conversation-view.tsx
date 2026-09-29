"use client";

import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { ChatLayout, ChatMessageList } from "@astryxdesign/core/Chat";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { app, type Conversation } from "@/schema";
import { regenerateReply, requestReply, resumeReply } from "./agent-api";
import { Composer } from "./composer";
import { AssistantMessage, UserMessage } from "./messages";
import { branchPath, latestLeaf, siblingsOf } from "./transcript";

const NEW_TITLE = "New conversation";

export function ConversationView({
  conversation,
  agentLabel,
}: {
  conversation: Conversation;
  agentLabel: string;
}) {
  const db = useDb();
  const where = { conversationId: conversation.id };
  const { data: turns = [] } = useAll(app.turns.where(where).select("*", "$createdAt"));
  const { data: toolCalls = [] } = useAll(app.toolCalls.where(where).orderBy("ordinal", "asc"));
  // Attachment metadata only: the audio bytes are fetched page by page by the player.
  const { data: attachments = [] } = useAll(
    app.attachments.where(where).select("turnId", "filename", "mediaType", "byteLength"),
  );
  const [error, setError] = useState<string>();

  const path = branchPath(turns, conversation.headTurnId);
  const head = path.at(-1);
  const replying =
    head?.role === "assistant" && (head.status === "queued" || head.status === "streaming");

  const act = (action: () => Promise<unknown>) => {
    setError(undefined);
    action().catch((cause) => setError(cause instanceof Error ? cause.message : String(cause)));
  };

  /** Show another branch: its newest leaf becomes the conversation's head. */
  const showBranch = (turnId: string) =>
    db.update(app.conversations, conversation.id, { headTurnId: latestLeaf(turns, turnId) });

  async function send(text: string, files: File[]) {
    // The browser writes the user's turn and audio into Jazz itself, then asks
    // the server to answer it. The reply streams back through Jazz.
    const userWrite = db.insert(app.turns, {
      conversationId: conversation.id,
      parentId: head?.id,
      role: "user",
      body: text,
      status: "complete",
    });
    const userTurnId = userWrite.value.id;
    const uploads = await Promise.all(
      files.map((file) =>
        db.insertStreaming(app.attachments, {
          conversationId: conversation.id,
          turnId: userTurnId,
          filename: file.name,
          mediaType: file.type || "application/octet-stream",
          byteLength: file.size,
          payload: file.stream(),
        }),
      ),
    );
    db.update(app.conversations, conversation.id, {
      headTurnId: userTurnId,
      ...(conversation.title === NEW_TITLE && text ? { title: titleFrom(text) } : {}),
    });
    await userWrite.wait({ tier: "global" });
    await Promise.all(uploads.map((upload) => upload.wait({ tier: "global" })));
    await requestReply(userTurnId);
  }

  return (
    <ChatLayout
      composer={
        <Composer
          isReplying={replying}
          error={error}
          onSend={(text, files) => act(() => send(text, files))}
          placeholder={
            replying
              ? "Replying. This keeps going if you close the tab."
              : `Ask ${agentLabel === "Claude" ? "Claude" : "the agent"} about venues, dates or a setlist`
          }
        />
      }
      emptyState={
        <EmptyState
          title="What are we booking?"
          description="Ask for venues in a city, the free dates on the calendar, or a setlist for a slot. Attach a rough mix to include it in the pitch."
        />
      }
    >
      {path.length > 0 ? (
        <ChatMessageList isStreaming={replying} align="top">
          {path.map((turn) => {
            const files = attachments.filter((file) => file.turnId === turn.id);
            if (turn.role === "user")
              return (
                <UserMessage
                  key={turn.id}
                  turn={turn}
                  attachments={files}
                  // A user turn at the head with no reply (the request failed) can ask again.
                  onRequestReply={
                    turn === head ? () => act(() => requestReply(turn.id)) : undefined
                  }
                />
              );
            const siblings = siblingsOf(turns, turn);
            return (
              <AssistantMessage
                key={turn.id}
                turn={turn}
                toolCalls={toolCalls.filter((call) => call.turnId === turn.id)}
                branch={{ index: siblings.indexOf(turn), count: siblings.length }}
                onShowBranch={(offset) => showBranch(siblings[siblings.indexOf(turn) + offset]!.id)}
                onRegenerate={() => act(() => regenerateReply(turn.id))}
                onResume={() => act(() => resumeReply(turn.id))}
              />
            );
          })}
        </ChatMessageList>
      ) : null}
    </ChatLayout>
  );
}

function titleFrom(text: string) {
  const line = text.split("\n")[0]!.trim();
  return line.length > 48 ? `${line.slice(0, 47)}…` : line;
}
