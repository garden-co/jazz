import { intro, outro, text, select, spinner, log, isCancel } from "@clack/prompts";
import pc from "picocolors";
import * as path from "node:path";
import { scaffold, validateAppName, type StarterName } from "./scaffold.js";
import { detectPackageManager } from "./detect-pm.js";

type Framework = "next" | "sveltekit";
type Auth = "localfirst" | "betterauth";

// Maps framework + auth choices to a starter directory under starters/.
// `null` means "not shipped yet" — the CLI surfaces a helpful error.
const STARTERS: Record<Framework, Record<Auth, StarterName | null>> = {
  next: {
    localfirst: "next-localfirst",
    betterauth: "next-betterauth",
  },
  sveltekit: {
    localfirst: null,
    betterauth: "sveltekit-betterauth",
  },
};

async function main() {
  const args = process.argv.slice(2);

  // Parse --starter <name> from argv — skips the interactive picker entirely.
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
    const framework = await select<Framework>({
      message: "Framework",
      options: [
        { value: "next", label: "React (Next.js)" },
        { value: "sveltekit", label: "Svelte (SvelteKit)" },
      ],
    });
    if (isCancel(framework)) process.exit(0);

    const auth = await select<Auth>({
      message: "Auth",
      options: [
        { value: "localfirst", label: "Local-first (anonymous, upgradeable)" },
        { value: "betterauth", label: "BetterAuth (email + password)" },
      ],
    });
    if (isCancel(auth)) process.exit(0);

    const picked = STARTERS[framework][auth];
    if (!picked) {
      log.error(`No starter available for ${framework} + ${auth} yet.`);
      process.exit(1);
    }
    starter = picked;
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
