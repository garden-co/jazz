type ImportMetaWithEnv = ImportMeta & {
  env?: Record<string, string | undefined>;
};

// Bundlers (Vite, Next/Webpack DefinePlugin, esbuild) only inline
// `process.env.X` / `import.meta.env.X` when both the object chain and the
// property name are literal in the source — computed keys, aliased env
// objects, and dynamic indexing all defeat static replacement.
export function resolveTelemetryCollectorUrlFromEnv(): string | undefined {
  const hasProcess = typeof process !== "undefined";
  return (
    trim(hasProcess ? process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim((import.meta as ImportMetaWithEnv).env?.VITE_JAZZ_TELEMETRY_COLLECTOR_URL)
  );
}

function trim(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}
