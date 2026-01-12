/**
 * Utility functions for Jazz CLI
 */

/**
 * Get the API key from environment variable
 */
export function getApiKey(): string {
  const apiKey = process.env.JAZZ_API_KEY;
  if (!apiKey) {
    console.error("Error: JAZZ_API_KEY environment variable not set");
    console.error("Set it with: export JAZZ_API_KEY=your-api-key");
    process.exit(1);
  }
  return apiKey;
}

/**
 * Format an error message (red)
 */
export function formatError(message: string): string {
  // Using ANSI escape codes for colors (works in most terminals)
  return `\x1b[31m${message}\x1b[0m`;
}

/**
 * Format a success message (green)
 */
export function formatSuccess(message: string): string {
  return `\x1b[32m${message}\x1b[0m`;
}

/**
 * Format a warning message (yellow)
 */
export function formatWarning(message: string): string {
  return `\x1b[33m${message}\x1b[0m`;
}

/**
 * Format an info message (blue)
 */
export function formatInfo(message: string): string {
  return `\x1b[34m${message}\x1b[0m`;
}
