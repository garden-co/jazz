/**
 * Surfaces a write's server receipt. Writes apply locally at once; this
 * waits for the global tier so a permission rejection is shown to the user
 * rather than silently rolled back.
 */
export type ReportWrite = (write: Promise<unknown>, subject: string) => Promise<void>;
