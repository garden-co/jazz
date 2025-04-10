import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      container: {
        center: true,
        padding: {
          DEFAULT: "0.75rem",
          sm: "1rem",
        },
        screens: {
          lg: "600px",
          xl: "600px",
        },
      },
    },
  },
  plugins: [],
} as const;

export default config;
