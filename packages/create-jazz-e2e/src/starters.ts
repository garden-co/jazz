/**
 * Per-starter configuration for the E2E harness. Each entry tells the
 * orchestrator (a) which env-var prefix the framework uses for `*_JAZZ_APP_ID`
 * and `*_JAZZ_SERVER_URL`, and (b) which extra env vars the starter needs at
 * build/run time (e.g. APP_ORIGIN for the Hono-server starters).
 *
 * Prod-start commands and ports live in each starter's playwright.config.ts,
 * keyed off `process.env.JAZZ_E2E_PROD === "1"`. We keep them there so each
 * starter's prod-serving choices stay co-located with its own config.
 */

export const KNOWN_STARTERS = [
  "next-betterauth",
  "next-localfirst",
  "next-hybrid",
  "sveltekit-betterauth",
  "sveltekit-localfirst",
  "sveltekit-hybrid",
  "react-betterauth",
  "react-localfirst",
  "react-hybrid",
  "ts-betterauth",
  "ts-localfirst",
  "ts-hybrid",
] as const;

export type StarterName = (typeof KNOWN_STARTERS)[number];

type EnvPrefix = "NEXT_PUBLIC" | "PUBLIC" | "VITE";

export interface StarterConfig {
  name: StarterName;
  envPrefix: EnvPrefix;
  /** Location of the schema module within the scaffolded starter. */
  schemaPath: string;
  /**
   * Origin the prod server will be reachable on (host + port). The orchestrator
   * sets this as APP_ORIGIN so any auth/JWT issuance code aimed at "this app"
   * targets the prod build, not the dev server's port.
   */
  appOrigin: string;
}

export function getStarterConfig(name: StarterName): StarterConfig {
  if (name.startsWith("next-")) {
    return {
      name,
      envPrefix: "NEXT_PUBLIC",
      schemaPath: "schema.ts",
      appOrigin: "http://localhost:3000",
    };
  }
  if (name.startsWith("sveltekit-")) {
    return {
      name,
      envPrefix: "PUBLIC",
      schemaPath: "src/lib/schema.ts",
      appOrigin: "http://localhost:5173",
    };
  }
  // react-* and ts-* both serve from Vite (preview) or a Hono server bound to
  // 5173, matching their playwright BASE_URL.
  return {
    name,
    envPrefix: "VITE",
    schemaPath: "schema.ts",
    appOrigin: "http://localhost:5173",
  };
}
