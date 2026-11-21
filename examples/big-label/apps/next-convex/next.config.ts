import type { NextConfig } from "next";

export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
} satisfies NextConfig;
