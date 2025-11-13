import { promises as fs } from "fs";
import { mkdirSync } from "fs";
import * as path from "path";

/**
 * A file artifact is a file that has been created or modified by an agent.
 */
export type FileArtifact = {
  path: string;
  content: string;
};

export function initWorkingDirectory({
  instanceId,
  projectName,
  agentName,
  outputDir = "output",
  baseDir = process.cwd(),
}: {
  projectName: string;
  instanceId: string;
  agentName: string;
  outputDir?: string;
  baseDir?: string;
}): string {
  const instanceDirectory = path.join(
    baseDir,
    outputDir,
    instanceId,
    projectName,
    agentName,
  );

  mkdirSync(instanceDirectory, { recursive: true });

  return instanceDirectory;
}

export async function saveFiles({
  files,
  workingDirectory,
}: {
  files: FileArtifact[];
  workingDirectory: string;
}): Promise<void> {
  for (const file of files) {
    const fullPath = path.join(workingDirectory, file.path);

    const dir = path.dirname(fullPath);
    await fs.mkdir(dir, { recursive: true });

    await fs.writeFile(fullPath, file.content, "utf-8");
  }
}

export async function readFileArtifacts({
  workingDirectory,
}: {
  workingDirectory: string;
}): Promise<FileArtifact[]> {
  const files: FileArtifact[] = [];

  const walk = async (dir: string) => {
    const entries = await fs.readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        await walk(fullPath);
      } else {
        files.push({
          path: fullPath.replace(workingDirectory, "").replace(/^\//, ""),
          content: await fs.readFile(fullPath, "utf-8"),
        });
      }
    }
  };

  await walk(workingDirectory);

  return files;
}
