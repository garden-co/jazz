/** Emails are stored and matched lower-cased and trimmed. */
export function normalizeEmail(email: string) {
  return email.trim().toLowerCase();
}
