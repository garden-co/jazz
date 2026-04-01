import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["jazz-napi", "jazz-tools"],
};

export default nextConfig;
