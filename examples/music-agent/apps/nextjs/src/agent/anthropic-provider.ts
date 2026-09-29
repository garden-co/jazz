import Anthropic from "@anthropic-ai/sdk";
import type { AgentProvider, GenerateInput } from "./provider";
import { isToolName, parseToolInput, toolDefinitions } from "./tools";

const MAX_TOOL_ROUNDS = 6;

const tools: Anthropic.Tool[] = toolDefinitions.map((tool) => ({
  ...tool,
  // Tool inputs stream as they are generated; each one is validated with
  // parseToolInput before it runs.
  eager_input_streaming: true,
}));

/**
 * Streams replies from the Claude API with the official SDK. Text deltas go to
 * the sink as they arrive; tool calls run against the workspace's Jazz data
 * and their results go back to the model until it finishes the reply.
 */
export function anthropicProvider(model: string): AgentProvider {
  const client = new Anthropic();
  return {
    id: "anthropic",
    label: "Claude",
    resume: "continue",
    async generate(input, sink) {
      const messages = toMessages(input);
      let wroteText = false;
      let paragraphBreak = false;
      for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
        const stream = client.messages.stream({
          model,
          max_tokens: 16000,
          system: systemPrompt(input),
          tools,
          messages,
        });
        for await (const event of stream) {
          if (event.type === "content_block_delta" && event.delta.type === "text_delta") {
            // Keep prose from separate model rounds in separate paragraphs.
            if (paragraphBreak) {
              await sink.text("\n\n");
              paragraphBreak = false;
            }
            await sink.text(event.delta.text);
            wroteText = true;
          }
        }
        const message = await stream.finalMessage();
        if (message.stop_reason === "refusal") {
          await sink.text("\n\nI can't help with that request.");
          return;
        }
        const toolUses = message.content.filter(
          (block): block is Anthropic.ToolUseBlock => block.type === "tool_use",
        );
        if (message.stop_reason !== "tool_use" || toolUses.length === 0) return;

        messages.push({ role: "assistant", content: message.content });
        const results: Anthropic.ToolResultBlockParam[] = [];
        for (const call of toolUses) {
          try {
            if (!isToolName(call.name)) throw new Error(`unknown tool ${call.name}`);
            const result = await sink.tool(call.name, parseToolInput(call.name, call.input));
            results.push({
              type: "tool_result",
              tool_use_id: call.id,
              content: JSON.stringify(result),
            });
          } catch (error) {
            results.push({
              type: "tool_result",
              tool_use_id: call.id,
              is_error: true,
              content: error instanceof Error ? error.message : String(error),
            });
          }
        }
        messages.push({ role: "user", content: results });
        paragraphBreak = wroteText;
      }
    },
  };
}

function systemPrompt({ artistName, tools }: GenerateInput) {
  return [
    `You are MusicAgent, the booking assistant working for the manager of ${artistName}.`,
    `Today is ${tools.today}.`,
    "Use the tools to look up venues, the artist's calendar and their catalogue rather than guessing.",
    "Be concise and practical. Use short Markdown lists for options, and suggest one next step.",
    "You can't listen to audio attachments; acknowledge them by filename when they are relevant.",
  ].join("\n");
}

/** The conversation path as Messages API turns; audio arrives as a note. */
function toMessages({ history, partialReply }: GenerateInput): Anthropic.MessageParam[] {
  const messages: Anthropic.MessageParam[] = history
    .filter((turn) => turn.text || turn.attachments.length)
    .map((turn) => ({
      role: turn.role,
      content: [
        turn.text,
        ...turn.attachments.map(
          (file) =>
            `[Attached ${file.mediaType} file "${file.filename}", ${file.byteLength} bytes]`,
        ),
      ]
        .filter(Boolean)
        .join("\n"),
    }));
  if (partialReply) {
    // The previous run stopped mid-reply. Show the model what was already
    // written and ask it to finish without repeating itself.
    messages.push(
      { role: "assistant", content: partialReply },
      {
        role: "user",
        content:
          "Your reply was cut off. Continue exactly where it stopped, without repeating anything.",
      },
    );
  }
  return messages;
}
