import "server-only";
import { anthropicProvider } from "./anthropic-provider";
import type { AgentProvider } from "./provider";
import { scriptedProvider } from "./scripted-provider";

export const DEFAULT_ANTHROPIC_MODEL = "claude-opus-5-5";

/**
 * The scripted agent is the default so the example runs offline and its
 * output is reproducible. Set MUSIC_AGENT_PROVIDER=anthropic (with
 * ANTHROPIC_API_KEY) to answer with Claude instead.
 */
export function agentProvider(): AgentProvider {
  const choice = process.env.MUSIC_AGENT_PROVIDER ?? "scripted";
  if (choice === "anthropic") {
    if (!process.env.ANTHROPIC_API_KEY)
      throw new Error("MUSIC_AGENT_PROVIDER=anthropic needs ANTHROPIC_API_KEY");
    return anthropicProvider(process.env.ANTHROPIC_MODEL || DEFAULT_ANTHROPIC_MODEL);
  }
  if (choice !== "scripted")
    throw new Error(`Unknown MUSIC_AGENT_PROVIDER "${choice}"; use scripted or anthropic`);
  return scriptedProvider(Number(process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS ?? 30));
}

/**
 * The label the UI shows for replies from the configured provider. A bad
 * configuration throws here too; instrumentation.ts validates it at boot so
 * the server never starts with a provider it can't run.
 */
export function agentLabel(): string {
  return agentProvider().label;
}
