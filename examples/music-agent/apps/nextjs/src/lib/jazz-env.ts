/**
 * Where the browser and the server find Jazz, and which environment they use.
 * Both sides must agree: the server seeds the workspace the browser reads.
 * Development falls back to the local dev server that `withJazz` starts;
 * production must configure every value, so a deploy never talks to a
 * localhost server or a shared local app id.
 */
function requiredInProduction(name: string, value: string | undefined, devDefault: string) {
  if (value) return value;
  if (process.env.NODE_ENV === "production") {
    throw new Error(`${name} must be configured in production`);
  }
  return devDefault;
}

// Read each NEXT_PUBLIC_ variable literally so Next inlines it into the browser bundle.
export const jazzServerUrl = requiredInProduction(
  "NEXT_PUBLIC_JAZZ_SERVER_URL",
  process.env.NEXT_PUBLIC_JAZZ_SERVER_URL,
  "http://127.0.0.1:4200",
);

export const jazzAppId = requiredInProduction(
  "NEXT_PUBLIC_JAZZ_APP_ID",
  process.env.NEXT_PUBLIC_JAZZ_APP_ID,
  "music-agent-local",
);

export const jazzEnv: "dev" | "prod" =
  process.env.NEXT_PUBLIC_JAZZ_ENV === "prod" ||
  (process.env.NEXT_PUBLIC_JAZZ_ENV !== "dev" && process.env.NODE_ENV === "production")
    ? "prod"
    : "dev";
