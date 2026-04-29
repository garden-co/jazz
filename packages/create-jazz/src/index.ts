import { intro, outro, text, select, spinner, log, isCancel } from "@clack/prompts";
import pc from "picocolors";
import * as path from "node:path";
import { scaffold, validateAppName, type StarterName, type ScaffoldOptions } from "./scaffold.js";
import { detectPackageManager } from "./detect-pm.js";
import { runHostedInit } from "./cloud-init.js";
import { writeBetterAuthSecret } from "./init-secret.js";

type Framework = "next" | "sveltekit";
type Hosting = "hosted" | "selfhosted";
type Auth = "localfirst" | "hybrid" | "betterauth";

// Maps framework + auth choices to a starter directory under starters/.
// `null` means "not shipped yet" — the CLI surfaces a helpful error.
const STARTERS: Record<Framework, Record<Auth, StarterName | null>> = {
  next: {
    localfirst: "next-localfirst",
    hybrid: "next-hybrid",
    betterauth: "next-betterauth",
  },
  sveltekit: {
    localfirst: "sveltekit-localfirst",
    hybrid: "sveltekit-hybrid",
    betterauth: "sveltekit-betterauth",
  },
};

const VALID_HOSTING_VALUES: Hosting[] = ["hosted", "selfhosted"];
const CLOUD_SYNC_URL = "https://v2.sync.jazz.tools/";

interface HostedEnvKeys {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  backendSecret: string;
}

const ENV_KEYS_BY_FRAMEWORK: Record<Framework | "react", HostedEnvKeys> = {
  next: {
    appId: "NEXT_PUBLIC_JAZZ_APP_ID",
    serverUrl: "NEXT_PUBLIC_JAZZ_SERVER_URL",
    adminSecret: "JAZZ_ADMIN_SECRET",
    backendSecret: "BACKEND_SECRET",
  },
  sveltekit: {
    appId: "PUBLIC_JAZZ_APP_ID",
    serverUrl: "PUBLIC_JAZZ_SERVER_URL",
    adminSecret: "JAZZ_ADMIN_SECRET",
    backendSecret: "BACKEND_SECRET",
  },
  react: {
    appId: "VITE_JAZZ_APP_ID",
    serverUrl: "VITE_JAZZ_SERVER_URL",
    adminSecret: "JAZZ_ADMIN_SECRET",
    backendSecret: "BACKEND_SECRET",
  },
};

export function envKeysForStarter(starter: string): HostedEnvKeys | null {
  if (starter.startsWith("next-")) return ENV_KEYS_BY_FRAMEWORK.next;
  if (starter.startsWith("sveltekit-")) return ENV_KEYS_BY_FRAMEWORK.sveltekit;
  if (starter.startsWith("react-")) return ENV_KEYS_BY_FRAMEWORK.react;
  return null;
}

function readFlagValue(args: string[], name: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const prefix = `--${name}=`;
    if (arg.startsWith(prefix)) return arg.slice(prefix.length);
    if (arg === `--${name}`) return args[i + 1];
  }
  return undefined;
}

async function main() {
  const args = process.argv.slice(2);

  // Parse --starter <name> or --starter=<name> — skips the interactive picker entirely.
  const starterArg = readFlagValue(args, "starter");

  // Parse --hosting hosted|selfhosted. Controls whether we provision a Jazz
  // Cloud app at scaffold time; otherwise the plugin spawns a local dev server.
  const rawHostingArg = readFlagValue(args, "hosting");

  const gitOptOut = args.includes("--no-git");

  // App name is the first non-flag argument
  const argvName = args.find((a) => !a.startsWith("-"));

  // #146aff brand blue via 24-bit ANSI escape
  const blue = (s: string) => `\x1b[38;2;20;106;255m${s}\x1b[39m`;
  intro(`${blue("♪")} ${pc.bold("Jazz")}`);

  if (rawHostingArg !== undefined && !VALID_HOSTING_VALUES.includes(rawHostingArg as Hosting)) {
    log.error(
      `Invalid --hosting value "${rawHostingArg}". Allowed values: ${VALID_HOSTING_VALUES.join(", ")}.`,
    );
    process.exit(1);
  }
  const hostingArg = rawHostingArg as Hosting | undefined;

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
  let hosting: Hosting;
  if (starterArg) {
    starter = starterArg;
    hosting = hostingArg ?? "selfhosted";
  } else if (process.stdout.isTTY) {
    const framework = await select<Framework>({
      message: "Framework",
      options: [
        { value: "next", label: "React (Next.js)" },
        { value: "sveltekit", label: "Svelte (SvelteKit)" },
      ],
    });
    if (isCancel(framework)) process.exit(0);

    const pickedHosting = await select<Hosting>({
      message: "Hosting",
      initialValue: "hosted" as Hosting,
      options: [
        { value: "hosted", label: "Hosted (Jazz Cloud)" },
        { value: "selfhosted", label: "Self-hosted" },
      ],
    });
    if (isCancel(pickedHosting)) process.exit(0);

    const auth = await select<Auth>({
      message: "Auth",
      options: [
        { value: "localfirst", label: "Local-first" },
        {
          value: "hybrid",
          label: "Hybrid (local-first + BetterAuth, optional upgrade to a managed account)",
        },
        {
          value: "betterauth",
          label: "BetterAuth (email + password, sign-up required)",
        },
      ],
    });
    if (isCancel(auth)) process.exit(0);

    const picked = STARTERS[framework][auth];
    if (!picked) {
      log.error(`No starter available for ${framework} + ${auth} yet.`);
      process.exit(1);
    }
    starter = picked;
    hosting = pickedHosting;
  } else {
    starter = "next-betterauth";
    hosting = hostingArg ?? "hosted";
  }

  const targetDir = path.resolve(appName);

  const pm = detectPackageManager(process.env.npm_config_user_agent);

  const needsBetterAuthSecret = starter.endsWith("-betterauth") || starter.endsWith("-hybrid");

  // Buffer log output emitted during scaffolding so it can be printed after
  // the spinner stops — otherwise clack's active-line redraw concatenates
  // it onto the current spinner message.
  const deferredLogs: { kind: "info" | "warn"; message: string }[] = [];

  const preInstall: ScaffoldOptions["preInstall"] =
    hosting === "hosted" || needsBetterAuthSecret
      ? async ({ dir, onStep }) => {
          if (needsBetterAuthSecret) {
            writeBetterAuthSecret(dir);
          }
          if (hosting === "hosted") {
            const envKeys = envKeysForStarter(starter);
            if (!envKeys) {
              deferredLogs.push({
                kind: "warn",
                message: `Skipping hosted provisioning: no env key map registered for starter "${starter}".`,
              });
            } else {
              await runHostedInit({
                dir,
                cloudSyncUrl: CLOUD_SYNC_URL,
                envKeys,
                onStep,
                onLog: (kind, message) => deferredLogs.push({ kind, message }),
              });
            }
          }
        }
      : undefined;

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
      preInstall,
    });

    s.stop("Done.");
  } catch (err) {
    s.stop(pc.red("Failed."));
    log.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  for (const entry of deferredLogs) {
    if (entry.kind === "warn") log.warn(entry.message);
    else log.info(entry.message);
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
