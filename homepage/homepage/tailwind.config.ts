import type { Config } from "tailwindcss";
import { pingColorMap } from "./components/cloud/pingColorThresholds";

export const colourSafelist = Object.values(pingColorMap).flatMap((value) => {
  const { light, dark } = value as { light: string; dark: string };
  return [`bg-[${light}]`, `dark:bg-[${dark}]`];
});

const config: Config = {
  presets: [require("@garden-co/design-system/tailwind.config.js")],
  darkMode: ["class"],
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./codeSamples/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./content/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./next.config.mjs",
    "./node_modules/@garden-co/design-system/src/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  safelist: [...colourSafelist],
};
export default config;
