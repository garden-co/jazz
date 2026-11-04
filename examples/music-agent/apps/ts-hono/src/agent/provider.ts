import type { ToolContext, ToolName } from "./tools";

/** One turn of the conversation path the agent answers, oldest first. */
export type HistoryTurn = {
  role: "user" | "assistant";
  text: string;
  attachments: { filename: string; mediaType: string; byteLength: number }[];
};

export type GenerateInput = {
  artistName: string;
  history: HistoryTurn[];
  tools: ToolContext;
  /** Set when an interrupted reply resumes: the prose already written. */
  partialReply?: string;
  /** Aborted when this runner loses the turn to another; stop generating. */
  signal?: AbortSignal;
};

/**
 * Where a provider sends its output. The runner turns every call into a Jazz
 * write, so each client watching the conversation sees the reply grow.
 */
export interface TurnSink {
  text(chunk: string): Promise<void>;
  tool(name: ToolName, input: Record<string, unknown>): Promise<unknown>;
}

export interface AgentProvider {
  id: "scripted" | "anthropic";
  /** Shown next to every reply, so a scripted answer is never mistaken for a model. */
  label: string;
  /**
   * How an interrupted reply resumes. `replay` providers are deterministic: the
   * runner replays them and skips what was already written. `continue`
   * providers receive `partialReply` and write only the rest.
   */
  resume: "replay" | "continue";
  generate(input: GenerateInput, sink: TurnSink): Promise<void>;
}
