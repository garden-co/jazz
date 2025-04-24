import type { Config } from "tailwindcss";

const config: Config = {
  presets: [require("@garden-co/design-system/tailwind.config.js")],
  darkMode: ["class"],
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./codeSamples/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./next.config.mjs",
    "./node_modules/@garden-co/design-system/src/**/*.{js,ts,jsx,tsx,mdx}",
  ],
};
export default config;
