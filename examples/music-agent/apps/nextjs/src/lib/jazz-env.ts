/**
 * Which Jazz app the browser and the server use. Both sides must agree: the
 * server seeds the workspace the browser reads. Development falls back to a
 * local app id; production must configure it.
 */
function requiredInProduction(name: string, value: string | undefined, devDefault: string) {
  if (value) return value;
  if (process.env.NODE_ENV === "production") {
    throw new Error(`${name} must be configured in production`);
  }
  return devDefault;
}

// Read the NEXT_PUBLIC_ variable literally so Next inlines it into the browser bundle.
export const jazzAppId = requiredInProduction(
  "NEXT_PUBLIC_JAZZ_APP_ID",
  process.env.NEXT_PUBLIC_JAZZ_APP_ID,
  "00000000-0000-0000-0000-00000000a6e7",
);
