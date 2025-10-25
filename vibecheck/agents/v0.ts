import "dotenv/config";

import { type AgentAdapter, AgentRole } from "@langwatch/scenario";
import { AssistantContent, AssistantModelMessage } from "ai";

import {
  type FileArtifact,
  initWorkingDirectory,
  saveFiles,
} from "../lib/file-artifact";
import { type CodingAgentFactory } from "../lib/coding-agent";

import { v0, type ChatDetail } from "v0-sdk";

const name = "v0" as const;

export const createV0Agent: CodingAgentFactory = ({
  projectName,
  instanceId,
}) => {
  let chat: ChatDetail | null = null;
  let files: FileArtifact[] = [];

  const adapter: AgentAdapter = {
    role: AgentRole.AGENT,
    call: async (input) => {
      const message = input.messages[input.messages.length - 1]
        .content as string;

      const newChat =
        chat === null
          ? await startV0Chat({ message, instanceId })
          : await sendMessageToV0({ message, chatId: chat.id });

      chat = newChat;
      files = mapV0Files(newChat);
      const agentResponse = mapV0Response(newChat);

      await saveFiles({
        files,
        workingDirectory: initWorkingDirectory({
          instanceId,
          projectName,
          agentName: name,
        }),
      });

      const filesResponse = filesAsResponse(files);

      return [agentResponse, filesResponse];
    },
  };

  const cleanupRemote = async () => {
    if (chat) {
      const project = await v0.projects.getByChatId({ chatId: chat.id });
      await v0.chats.delete({ chatId: chat.id });
      if (project) await v0.projects.delete({ projectId: project.id });
    }
  };

  return {
    name,
    adapter,
    get files() {
      return files;
    },
    cleanupRemote,
  };
};

const startV0Chat = async ({
  message,
  instanceId,
}: {
  message: string;
  instanceId: string;
}): Promise<ChatDetail> => {
  const project = await v0.projects.create({
    name: `[TEST] ${instanceId}`,
    privacy: "team",
  });

  return (await v0.chats.create({
    message,
    chatPrivacy: "team",
    responseMode: "sync",
    projectId: project.id,
  })) as unknown as ChatDetail;
};

const sendMessageToV0 = async ({
  chatId,
  message,
}: {
  chatId: string;
  message: string;
}): Promise<ChatDetail> => {
  return (await v0.chats.sendMessage({
    chatId,
    message,
  })) as unknown as ChatDetail;
};

const mapV0Files = (chat: ChatDetail): FileArtifact[] => {
  return (
    chat.files?.map((file) => ({
      path: (file.meta.file ||
        file.meta.fileName ||
        file.meta.filePath) as string,
      content: file.source,
    })) || []
  );
};

const mapV0Response = (chat: ChatDetail): AssistantModelMessage => {
  const response = [...chat.messages]
    .reverse()
    .find((msg) => msg.role === "assistant");

  return {
    role: "assistant",
    content: splitContentParts(response?.content as string),
  };
};

function splitContentParts(input: string): AssistantContent {
  const regex = /<Thinking>([\s\S]*?)<\/Thinking>/gi;
  const result: AssistantContent = [];
  let lastIndex = 0;
  let match;

  while ((match = regex.exec(input)) !== null) {
    // Add preceding plain text
    if (match.index > lastIndex) {
      result.push({
        type: "text",
        text: input.slice(lastIndex, match.index).trim(),
      });
    }

    // Add thinking block
    result.push({
      type: "reasoning",
      text: match[1].trim(),
    });

    lastIndex = regex.lastIndex;
  }

  // Add trailing text
  if (lastIndex < input.length) {
    result.push({
      type: "text",
      text: input.slice(lastIndex).trim(),
    });
  }

  if (result.length === 0 && result[0].type === "text") {
    return result[0].text;
  }

  return result.filter((part) =>
    "text" in part ? part.text.trim() !== "" : true,
  );
}

/**
 * Vercel doesn't return file contents in the messages, so this
 * serializes the present files as an assistant message.
 */
const filesAsResponse = (files: FileArtifact[]): AssistantModelMessage => {
  return {
    role: "assistant",
    content: [
      { type: "text", text: "Here are all the present files:" },
      ...files.map((file) => ({
        type: "text" as const,
        text: `FILE: ${file.path}\nCONTENT: ${file.content}`,
      })),
    ],
  };
};

globalThis.AI_SDK_LOG_WARNINGS = false;
