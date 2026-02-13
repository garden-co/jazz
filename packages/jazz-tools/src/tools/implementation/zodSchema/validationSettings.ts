import { z } from "./zodReExport.js";

/**
 * Global validation mode that can include "warn" for logging-only validation.
 * - "strict": validate and throw on error (default)
 * - "loose": skip validation entirely
 * - "warn": validate but only log errors to console (don't throw)
 */
export type GlobalValidationMode = "strict" | "loose" | "warn";

/**
 * Local validation mode for per-operation overrides.
 * Only supports "strict" | "loose" - "warn" is only available as a global setting.
 */
export type LocalValidationMode = "strict" | "loose";

/**
 * The current global validation mode.
 * @default "strict"
 */
export let DEFAULT_VALIDATION_MODE: GlobalValidationMode = "warn";

/**
 * Set the default validation mode for all CoValue operations.
 * This affects create, set, push, and other mutation operations across
 * CoMap, CoList, and CoFeed.
 *
 * @param mode - The validation mode to set:
 *   - "strict": validate and throw on error (default)
 *   - "loose": skip validation entirely
 *   - "warn": validate but only log errors to console
 *
 * @example
 * ```ts
 * import { setDefaultValidationMode } from "jazz-tools";
 *
 * // Disable validation globally during development
 * setDefaultValidationMode("loose");
 *
 * // Enable warning-only mode to see validation issues without breaking
 * setDefaultValidationMode("warn");
 *
 * // Re-enable strict validation
 * setDefaultValidationMode("strict");
 * ```
 */
export function setDefaultValidationMode(mode: GlobalValidationMode): void {
  shouldShout = false;
  DEFAULT_VALIDATION_MODE = mode;
}

/**
 * Get the current default validation mode.
 *
 * @returns The current global validation mode
 */
export function getDefaultValidationMode(): GlobalValidationMode {
  return DEFAULT_VALIDATION_MODE;
}

let shouldShout = true;
/**
 * Resolve the effective validation mode based on local override and global default.
 * Local overrides take precedence over the global setting.
 *
 * @param localOverride - Optional local validation mode ("strict" | "loose")
 * @returns The effective validation mode to use
 */
export function resolveValidationMode(
  localOverride?: LocalValidationMode,
): GlobalValidationMode {
  if (shouldShout) {
    console.warn(
      "Validation mode is %s by default, but in the next major version it will be strict",
      DEFAULT_VALIDATION_MODE,
    );
    shouldShout = false;
  }
  return localOverride ?? DEFAULT_VALIDATION_MODE;
}

/**
 * Execute validation with the specified mode.
 * Centralizes validation logic to handle strict, loose, and warn modes consistently.
 *
 * @param schema - The Zod schema to validate against
 * @param value - The value to validate
 * @param mode - The validation mode to use
 * @returns The validated (and possibly transformed) value
 */
export function executeValidation<T>(
  schema: z.ZodType<T>,
  value: unknown,
  mode: GlobalValidationMode,
): T {
  if (mode === "loose") {
    return value as T;
  }

  const result = z.safeParse(schema, value);

  if (!result.success) {
    if (mode === "warn") {
      console.warn("[Jazz] Validation warning:", result.error);
      return value as T;
    }

    // mode === "strict"
    throw result.error;
  }

  return result.data;
}
