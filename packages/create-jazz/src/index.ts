import { intro, outro, text, select, spinner, log, isCancel } from "@clack/prompts";
import pc from "picocolors";
import * as path from "node:path";
import { scaffold, validateAppName, KNOWN_STARTERS } from "./scaffold.js";
import { detectPackageManager } from "./detect-pm.js";

async function main() {
  const args = process.argv.slice(2);

  // Parse --starter <name> from argv
  const starterFlagIndex = args.indexOf("--starter");
  const starterArg = starterFlagIndex !== -1 ? args[starterFlagIndex + 1] : undefined;
  const gitOptOut = args.includes("--no-git");

  // App name is the first non-flag argument
  const argvName = args.find((a) => !a.startsWith("-"));

  intro(pc.bold("create-jazz"));

  let appName: string;
  if (argvName) {
    appName = argvName;
  } else {
    const answer = await text({
      message: "What's your app called?",
      placeholder: "my-app",
      validate: (value) => {
        try {
          validateAppName(value);
          return undefined;
        } catch (err) {
          return err instanceof Error ? err.message : String(err);
        }
      },
    });
    if (typeof answer !== "string" || !answer) process.exit(0);
    appName = answer;
  }

  try {
    validateAppName(appName);
  } catch (err) {
    log.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  let starter: string;
  if (starterArg) {
    starter = starterArg;
  } else if (process.stdout.isTTY) {
    const answer = await select({
      message: "Which starter would you like to use?",
      options: KNOWN_STARTERS.map((s) => ({ value: s, label: s })),
    });
    if (isCancel(answer)) process.exit(0);
    starter = answer as string;
  } else {
    starter = "next-betterauth";
  }

  const targetDir = path.resolve(appName);

  const pm = detectPackageManager(process.env.npm_config_user_agent);

  const s = spinner();

  try {
    s.start("Scaffolding project…");

    await scaffold({
      appName,
      targetDir,
      pm,
      starter,
      git: !gitOptOut,
      onStep: (label) => s.message(label),
    });

    s.stop("Done.");
  } catch (err) {
    s.stop(pc.red("Failed."));
    log.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  if (pm) {
    const devCmd = pm === "npm" ? "npm run dev" : `${pm} dev`;
    outro(`
  Next steps:
    cd ${appName}
    ${devCmd}
`);
  } else {
    outro(`
  Next steps:
    cd ${appName}
    npm install && npm run dev
  (or substitute your preferred package manager)
`);
  }
}

main();
