import { type AgentAdapter } from "@langwatch/scenario";
import { FileArtifact } from "./file-artifact";

export interface CodingAgent {
  name: string;
  adapter: AgentAdapter;
  files: FileArtifact[];
  cleanupRemote: () => Promise<void>;
}

export type CodingAgentFactory = (params: {
  projectName: string;
  instanceId: string;
}) => CodingAgent;
