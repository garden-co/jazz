import * as pty from "node-pty";
import path, { matchesGlob } from "path";

import { type AgentAdapter, AgentRole } from "@langwatch/scenario";
import { SDKUserMessage, type SDKMessage } from "@anthropic-ai/claude-code";
import { BetaContentBlock } from "@anthropic-ai/sdk/resources/beta/messages/messages.mjs";
import { ContentBlockParam } from "@anthropic-ai/sdk/resources";
import {
  type AssistantContent,
  type TextPart,
  type ToolCallPart,
  type UserContent,
  type ModelMessage,
  type FilePart,
  type ImagePart,
  type ToolContent,
  type ToolResultPart,
  type ReasoningPart,
} from "@ai-sdk/provider-utils";

import {
  type FileArtifact,
  initWorkingDirectory,
  readFileArtifacts,
} from "../lib/file-artifact";
import { type CodingAgentFactory } from "../lib/coding-agent";

const name = "claude" as const;

export const createClaudeAgent: CodingAgentFactory = ({
  projectName,
  instanceId,
}) => {
  let files: FileArtifact[] = [];

  const adapter: AgentAdapter = {
    role: AgentRole.AGENT,
    call: async (input) => {
      const formattedMessages = input.messages
        .map((message) => `${message.role}: ${message.content}`)
        .join("\n\n");

      return new Promise<ModelMessage[]>((resolve, reject) => {
        const claudeBin = path.join(
          __dirname,
          "../../node_modules/.bin/claude", // bin is in the root of the project
        );

        const claudeCliArgs = [
          "--output-format",
          "stream-json",
          "--print", // respond w/o interactive mode
          "--dangerously-skip-permissions",
          "--verbose",
          formattedMessages,
        ];

        const workingDirectory = initWorkingDirectory({
          instanceId,
          projectName,
          agentName: name,
        });

        const claudeProcess = pty.spawn(claudeBin, claudeCliArgs, {
          name: "xterm-256color",
          cols: 80,
          rows: 30,
          cwd: workingDirectory,
          // env: {},
        });

        let claudeOutput = "";

        claudeProcess.onData((data) => {
          claudeOutput += data;
        });

        claudeProcess.onExit(({ exitCode }) => {
          if (exitCode === 0) {
            readFileArtifacts({ workingDirectory }).then((foundFiles) => {
              files = foundFiles;
              const claudeMessages = parseClaudeOutput(claudeOutput);
              const response = mapClaudeResponse(claudeMessages);

              resolve(response);
            });
          } else {
            console.error("CLAUDE CLI OUTPUT:\n", claudeOutput);

            const claudeMessages = parseClaudeOutput(claudeOutput);
            if (claudeMessages.length > 0) {
              const error = mapClaudeError(claudeMessages);
              reject(new Error(`Claude CLI errored: ${error}`));
            } else {
              reject(new Error(`Claude CLI exited with code ${exitCode}`));
            }
          }
        });
      });
    },
  };

  return {
    name,
    adapter,
    get files() {
      return files;
    },
    cleanupRemote: async () => {
      // No remote resources to cleanup, Claude Code works locally
    },
  };
};

const mapClaudeResponse = (messages: SDKMessage[]): ModelMessage[] => {
  return messages
    .filter(
      (msg) =>
        msg.type === "user" ||
        msg.type === "assistant" ||
        msg.type === "result",
    )
    .map<ModelMessage[]>((msg) => {
      if (msg.type === "result") {
        return msg.subtype === "success"
          ? [{ role: "assistant", content: msg.result }]
          : [{ role: "assistant", content: msg.subtype }];
      }

      if (msg.type === "assistant") {
        return [
          {
            role: "assistant",
            content: mapClaudeAssistantContent(msg.message.content),
          },
        ];
      }

      return recoverToolResultFromUser(msg).map(({ message, kind }) =>
        kind === "user"
          ? {
              role: "user",
              content: mapClaudeUserContent(message.message.content),
            }
          : {
              role: "tool",
              content: mapClaudeToolContent(
                message.message.content as ContentBlockParam[],
              ),
            },
      );
    })
    .flatMap((msg) => msg);
};

const mapClaudeAssistantContent = (
  contentBlocks: BetaContentBlock[],
): AssistantContent => {
  if (typeof contentBlocks === "string") return contentBlocks;

  return contentBlocks.map((block) => {
    if (block.type === "text")
      return { type: "text", text: block.text } as TextPart;
    if (
      block.type === "tool_use" ||
      block.type === "mcp_tool_use" ||
      block.type === "server_tool_use"
    )
      return {
        type: "tool-call",
        toolCallId: block.id,
        toolName: block.name,
        input: block.input,
      } as ToolCallPart;
    if (block.type === "thinking" || block.type === "redacted_thinking")
      return {
        type: "reasoning",
        text: block.type === "thinking" ? block.thinking : block.data,
      } as ReasoningPart;
    if (block.type === "container_upload")
      return {
        type: "tool-call",
        toolCallId: "file-upload",
        toolName: "container-upload",
        input: block.file_id,
      } as ToolCallPart;

    // This should never happen (unhandled types should not be used in ASSISTANT messages)
    throw new Error(`Unknown assistant content block type: ${block.type}`);
  });
};

const mapClaudeUserContent = (
  contentBlocks: string | ContentBlockParam[],
): UserContent => {
  if (typeof contentBlocks === "string") return contentBlocks;

  return contentBlocks.map((block) => {
    if (block.type === "text")
      return { type: "text", text: block.text } as TextPart;
    if (block.type === "image")
      return {
        type: "image",
        image:
          block.source.type === "base64" ? block.source.data : block.source.url,
      } as ImagePart;
    if (block.type === "document")
      return {
        type: "file",
        data:
          block.source.type === "base64"
            ? block.source.data
            : block.source.type === "url"
              ? new URL(block.source.url)
              : block.source.type === "content"
                ? JSON.stringify(block.source.content)
                : block.source.data,
        mediaType:
          block.source.type === "base64"
            ? block.source.media_type
            : block.source.type === "text"
              ? block.source.media_type
              : block.source.type === "url"
                ? "url"
                : "application/pdf",
      } as FilePart;

    // This should never happen (unhandled types should not be used in USER messages)
    throw new Error(`Unknown user content block type: ${block.type}`);
  });
};

const mapClaudeToolContent = (
  contentBlocks: ContentBlockParam[],
): ToolContent => {
  return contentBlocks.map((block) => {
    if (block.type === "tool_result")
      return {
        type: "tool-result",
        toolCallId: block.tool_use_id,
        toolName: block.tool_use_id,
        output:
          typeof block.content === "string"
            ? { type: "text" as const, value: block.content }
            : { type: "json" as const, value: block.content },
      } as ToolResultPart;

    if (block.type === "search_result")
      return {
        type: "tool-result",
        toolCallId: `search-${Math.random().toString(36).substring(2, 15)}`,
        toolName: "search",
        output: {
          type: "text" as const,
          value: block.content.map((c) => c.text).join("\n\n"),
        },
      } as ToolResultPart;

    // This should never happen (unhandled types should not be used in TOOL messages)
    throw new Error(`Unknown tool content block type: ${block.type}`);
  });
};

/**
 * This splits `role=user` message content parts
 * into `role=user` and `role=tool` messages.
 *
 * Claude includes tool results in the "role=user" message, but they
 * should be their own "role=tool" message.
 */
const recoverToolResultFromUser = (
  message: SDKUserMessage,
): Array<{ message: SDKUserMessage; kind: "user" | "tool-result" }> => {
  if (typeof message.message.content === "string")
    return [{ message, kind: "user" }];

  const RESULT_TYPES: ContentBlockParam["type"][] = [
    "tool_result",
    "search_result",
  ];

  const outputMessages: Array<{
    message: SDKUserMessage;
    kind: "user" | "tool-result";
  }> = [];

  for (const block of message.message.content) {
    if (RESULT_TYPES.includes(block.type)) {
      outputMessages.push({ message, kind: "tool-result" });
    } else {
      outputMessages.push({ message, kind: "user" });
    }
  }

  return outputMessages;
};

const mapClaudeError = (messages: SDKMessage[]): string | null => {
  const resultMessage = messages.find((msg) => msg.type === "result");

  if (resultMessage)
    return resultMessage.subtype === "success"
      ? resultMessage.result
      : resultMessage.subtype;

  const lastAssistantMessage = messages
    .slice()
    .reverse()
    .find((msg) => msg.type === "assistant");

  if (!lastAssistantMessage) return null;

  const assistantContent = mapClaudeAssistantContent(
    lastAssistantMessage.message.content,
  );

  if (typeof assistantContent === "string") return assistantContent;

  const textContent = assistantContent
    .filter((c) => c.type === "text")
    .map((c) => c.text)
    .join("\n")
    .trim();

  return textContent || null;
};

const parseClaudeOutput = (output: string): SDKMessage[] =>
  output
    .split("\n")
    .map((line) => {
      try {
        return JSON.parse(line.trim()) as SDKMessage;
      } catch (error) {
        return null;
      }
    })
    .filter((msg) => msg !== null);
