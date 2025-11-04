import tailwindPreset from "../src/config/tailwind.config";

/** @type {import('tailwindcss').Config} */
export default {
  presets: [tailwindPreset],
  content: ["./demo/**/*.{ts,tsx}", "./src/components/**/*.{ts,tsx}"],
};
