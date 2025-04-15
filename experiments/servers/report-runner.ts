import { exec } from "child_process";
import { readdirSync, statSync } from "fs";
import { join } from "path";

const VALID_SUFFIXES = new Set(
    Array.from({ length: 10 }, (_, i) => `.json.${(i + 1).toString().padStart(2, "0")}`)
);

function findTargetFiles(dir: string, foundFiles: string[] = []): string[] {
  const items = readdirSync(dir);
  console.log(`Folder items found: ${items.length}`);

  for (const item of items) {
    const fullPath = join(dir, item);
    const stats = statSync(fullPath);

    if (stats.isDirectory()) {
      findTargetFiles(fullPath, foundFiles); // recurse into subdirectories
    } else {
        for (const suffix of VALID_SUFFIXES) {
            if (item.endsWith(suffix)) {
              foundFiles.push(fullPath);
              break;
            }
        }
    }
  }

  return foundFiles;
}

function runArtilleryReport(filePath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const command = `pnpm dlx artillery@2.0.21 report "${filePath}"`;
    console.log(`Running: ${command}`);

    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error(`Error running report for ${filePath}:`, stderr);
        return reject(error);
      }

      console.log(`Report generated for ${filePath}:\n`, stdout);
      resolve();
    });
  });
}

async function main() {
  const baseDir = process.argv[2] || ".";
  console.log(`Searching for target files in: ${baseDir}`);

  const files = findTargetFiles(baseDir);
  console.log(`Found ${files.length} target file(s).`);

  for (const file of files) {
    try {
      await runArtilleryReport(file);
    } catch (err) {
      console.error(`Failed to process ${file}`);
    }
  }
}

main();
