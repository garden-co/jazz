/** Metro/Hermes has no import.meta. Keep Expo's statically replaced env access. */
export function resolveTelemetryCollectorUrlFromEnv(): string | undefined {
  if (typeof process === "undefined") return undefined;
  return process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL?.trim() || undefined;
}
