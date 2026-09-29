"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import {
  ChatMessage,
  ChatMessageBubble,
  ChatMessageMetadata,
  ChatToolCalls,
  type ChatToolCallItem,
} from "@astryxdesign/core/Chat";
import { CodeBlock } from "@astryxdesign/core/CodeBlock";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { Markdown } from "@astryxdesign/core/Markdown";
import { Spinner } from "@astryxdesign/core/Spinner";
import { HStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import type { ToolCall, Turn } from "@/schema";
import { AttachmentPlayer, type AttachmentSummary } from "./attachment-player";

export function UserMessage({
  turn,
  attachments,
  onRequestReply,
}: {
  turn: Turn;
  attachments: AttachmentSummary[];
  onRequestReply?: () => void;
}) {
  return (
    <ChatMessage sender="user">
      {attachments.map((file) => (
        <AttachmentPlayer key={file.id} attachment={file} />
      ))}
      <ChatMessageBubble metadata={<ChatMessageMetadata timestamp={<SentAt turn={turn} />} />}>
        {turn.body}
      </ChatMessageBubble>
      {onRequestReply && (
        <HStack justify="end">
          <Button size="sm" label="Ask for a reply" onClick={onRequestReply} />
        </HStack>
      )}
    </ChatMessage>
  );
}

export function AssistantMessage({
  turn,
  toolCalls,
  branch,
  onShowBranch,
  onRegenerate,
  onResume,
}: {
  turn: Turn;
  toolCalls: ToolCall[];
  branch: { index: number; count: number };
  onShowBranch: (offset: -1 | 1) => void;
  onRegenerate: () => void;
  onResume: () => void;
}) {
  const working = turn.status === "queued" || turn.status === "streaming";
  const footer = (
    <HStack gap={1} align="center" wrap="wrap">
      <Text type="supporting" color="secondary">
        {turn.provider ?? "Agent"}
      </Text>
      {branch.count > 1 && (
        <HStack gap={0.5} align="center">
          <IconButton
            size="sm"
            variant="ghost"
            label="Previous reply"
            icon={<Icon icon="chevronLeft" size="sm" />}
            isDisabled={branch.index === 0}
            onClick={() => onShowBranch(-1)}
          />
          <Text type="supporting" color="secondary" hasTabularNumbers>
            {`${branch.index + 1} of ${branch.count}`}
          </Text>
          <IconButton
            size="sm"
            variant="ghost"
            label="Next reply"
            icon={<Icon icon="chevronRight" size="sm" />}
            isDisabled={branch.index === branch.count - 1}
            onClick={() => onShowBranch(1)}
          />
        </HStack>
      )}
      {!working && (
        <Button
          size="sm"
          variant="ghost"
          label="Regenerate"
          tooltip="Answer again as a new branch"
          onClick={onRegenerate}
        />
      )}
    </HStack>
  );

  return (
    <ChatMessage sender="assistant" name="MusicAgent">
      {toolCalls.length > 0 && <ChatToolCalls calls={toolCalls.map(toolCallItem)} />}
      <ChatMessageBubble
        metadata={<ChatMessageMetadata timestamp={<SentAt turn={turn} />} footer={footer} />}
      >
        {turn.body ? (
          <Markdown isStreaming={turn.status === "streaming"}>{turn.body}</Markdown>
        ) : working ? (
          <Spinner
            size="sm"
            label={turn.status === "queued" ? "Waiting for the agent" : "Thinking"}
          />
        ) : (
          <Text color="secondary">No reply was written.</Text>
        )}
      </ChatMessageBubble>
      {turn.status === "interrupted" && (
        <Banner
          status="warning"
          title="Reply interrupted"
          description="The server stopped while writing this reply. Resume it from where it stopped, or regenerate it as a new branch."
          endContent={<RecoveryActions onResume={onResume} onRegenerate={onRegenerate} />}
        />
      )}
      {turn.status === "failed" && (
        <Banner
          status="error"
          title="Reply failed"
          description={turn.error ?? undefined}
          endContent={<RecoveryActions onResume={onResume} onRegenerate={onRegenerate} />}
        />
      )}
    </ChatMessage>
  );
}

function RecoveryActions({
  onResume,
  onRegenerate,
}: {
  onResume: () => void;
  onRegenerate: () => void;
}) {
  return (
    <HStack gap={2} wrap="wrap">
      <Button size="sm" variant="primary" label="Resume" onClick={onResume} />
      <Button size="sm" label="Regenerate" onClick={onRegenerate} />
    </HStack>
  );
}

function SentAt({ turn }: { turn: Turn }) {
  return <Timestamp value={new Date(turn.createdAt).getTime()} format="time" />;
}

function toolCallItem(call: ToolCall): ChatToolCallItem {
  const args = safeParse(call.argumentsJson) as Record<string, unknown> | undefined;
  const result = call.resultJson ? safeParse(call.resultJson) : undefined;
  return {
    key: call.id,
    name: call.name,
    status: call.status,
    target: args ? describeArguments(args) : undefined,
    duration:
      call.durationMs === undefined || call.durationMs === null
        ? undefined
        : `${call.durationMs} ms`,
    errorMessage:
      call.status === "error" && result && typeof result === "object" && "error" in result
        ? String(result.error)
        : undefined,
    resultDetail:
      result === undefined ? undefined : (
        <CodeBlock
          code={JSON.stringify(result, null, 2)}
          language="json"
          size="sm"
          isCollapsible
          isWrapped
        />
      ),
  };
}

function describeArguments(args: Record<string, unknown>) {
  const values = Object.entries(args).map(([key, value]) =>
    key === "minutes" ? `${value} min` : String(value),
  );
  return values.length ? values.join(", ") : "all";
}

function safeParse(json: string): unknown {
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
}
