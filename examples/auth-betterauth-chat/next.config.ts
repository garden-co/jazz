import { withJazz } from "jazz-tools/dev/next";

export const baseNextConfig = {
  reactStrictMode: true,
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};

export const jazzOptions = {
  server: false as const,
};

export default withJazz(baseNextConfig, jazzOptions);
