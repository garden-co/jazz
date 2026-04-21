const RECOGNISED = new Set(["pnpm", "yarn", "npm", "bun"]);

export function detectPackageManager(userAgent: string | undefined): string | null {
  if (!userAgent) return null;
  const name = userAgent.split("/")[0].toLowerCase();
  return RECOGNISED.has(name) ? name : null;
}
