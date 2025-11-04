import type { Config } from "tailwindcss";
import harmonyPalette from "@evilmartians/harmony/tailwind";
import typography from "@tailwindcss/typography";
import tailwindCSSAnimate from "tailwindcss-animate";
import colors from "tailwindcss/colors";
import plugin from "tailwindcss/plugin";
import { COLORS } from "@garden-co/design-system/colors/colors";

const stonePalette = {
  50: "oklch(0.988281 0.002 75)",
  100: "oklch(0.980563 0.002 75)",
  200: "oklch(0.917969 0.002 75)",
  300: "oklch(0.853516 0.002 75)",
  400: "oklch(0.789063 0.002 75)",
  500: "oklch(0.726563 0.002 75)",
  600: "oklch(0.613281 0.002 75)",
  700: "oklch(0.523438 0.002 75)",
  800: "oklch(0.412109 0.002 75)",
  900: "oklch(0.302734 0.002 75)",
  925: "oklch(0.220000 0.002 75)",
  950: "oklch(0.193359 0.002 75)",
};

const jazzBlue = {
  ...colors.indigo,
  500: "#5870F1",
  600: "#3651E7",
  700: "#3313F7",
  800: "#2A12BE",
  900: "#12046A",
  DEFAULT: COLORS.BLUE,
};

const green = {
  ...colors.green,
  DEFAULT: COLORS.FOREST,
};

const cyan = {
  ...colors.cyan,
  DEFAULT: COLORS.TURQUOISE,
};

const red = {
  ...colors.red,
  DEFAULT: COLORS.RED,
};

const yellow = {
  ...colors.yellow,
  DEFAULT: COLORS.YELLOW,
};

const orange = {
  ...colors.orange,
  DEFAULT: COLORS.ORANGE,
};

const purple = {
  ...colors.purple,
  DEFAULT: COLORS.PURPLE,
};

const stonePaletteWithAlpha: Record<string, string> = { ...stonePalette };

Object.keys(stonePalette).forEach((key) => {
  stonePaletteWithAlpha[key] = stonePaletteWithAlpha[key].replace(
    ")",
    "/ <alpha-value>)",
  );
});

const config: Config = {
  darkMode: "class",
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./codeSamples/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./content/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./next.config.mjs",
    "../design-system/src/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        ...harmonyPalette,
        stone: stonePaletteWithAlpha,
        blue: jazzBlue,
        green,
        cyan,
        red,
        yellow,
        purple,
        orange,
        muted: "var(--color-muted)",
        strong: "var(--color-strong)",
        primary: {
          DEFAULT: "var(--color-primary)",
          transparent: "var(--color-transparent-primary)",
          dark: "var(--color-primary-dark)",
          light:
            "lch(from var(--color-primary) calc(l + 10) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-primary) calc(l - 1) calc(c + 20) calc(h + 5))",
          "bright-dark":
            "lch(from var(--color-primary) calc(l - 6) calc(c + 20) calc(h + 5))",
        },
        success: {
          DEFAULT: "var(--color-success)",
          transparent: "lch(from var(--color-success) l c h / 0.3)",
          dark: "lch(from var(--color-success) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-success) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-success) calc(l - 1) calc(c + 20) calc(h + 10))",
          "bright-dark":
            "lch(from var(--color-success) calc(l - 6) calc(c + 20) calc(h + 10))",
        },
        info: {
          DEFAULT: "var(--color-info)",
          transparent: "lch(from var(--color-info) l c h / 0.3)",
          dark: "lch(from var(--color-info) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-info) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-info) calc(l - 1) calc(c + 20) calc(h + 5))",
          "bright-dark":
            "lch(from var(--color-info) calc(l - 4) calc(c + 20) calc(h + 5))",
        },
        warning: {
          DEFAULT: "var(--color-warning)",
          transparent: "lch(from var(--color-warning) l c h / 0.3)",
          dark: "lch(from var(--color-warning) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-warning) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-warning) calc(l - 1) calc(c + 30) calc(h + 15))",
          "bright-dark":
            "lch(from var(--color-warning) calc(l - 4) calc(c + 30) calc(h + 15))",
        },
        danger: {
          DEFAULT: "var(--color-danger)",
          transparent: "lch(from var(--color-danger) l c h / 0.3)",
          dark: "lch(from var(--color-danger) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-danger) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-danger) calc(l - 2) calc(c + 20) calc(h + 10))",
          "bright-dark":
            "lch(from var(--color-danger) calc(l - 6) calc(c + 10) calc(h + 10))",
        },
        tip: {
          DEFAULT: "var(--color-tip)",
          transparent: "lch(from var(--color-tip) l c h / 0.3)",
          dark: "lch(from var(--color-tip) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-tip) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-tip) calc(l - 1) calc(c + 20) calc(h + 10))",
          "bright-dark":
            "lch(from var(--color-tip) calc(l - 4) calc(c + 20) calc(h + 10))",
        },
        alert: {
          DEFAULT: "var(--color-alert)",
          transparent: "lch(from var(--color-alert) l c h / 0.3)",
          dark: "lch(from var(--color-alert) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-alert) calc(l + 4) calc(c + 1) calc(h - 5))",
          "bright-light":
            "lch(from var(--color-alert) calc(l - 1) calc(c + 50) calc(h + 15))",
          "bright-dark":
            "lch(from var(--color-alert) calc(l - 5) calc(c + 50) calc(h + 15))",
        },
      },
      textColor: {
        default: "var(--color-default)",
        highlight: "var(--color-highlight)",
        strong: "var(--color-strong)",
        muted: "var(--color-muted)",
      },
      borderColor: {
        DEFAULT: "var(--color-border-default)",
      },
      backgroundColor: {
        highlight: "var(--color-background-highlight)",
      },
      fontFamily: {
        display: ["var(--font-manrope)"],
        mono: ["var(--font-commit-mono)"],
        sans: ["var(--font-inter)"],
      },
      fontSize: {
        "2xs": ["0.75rem", { lineHeight: "1.25rem" }],
      },
      container: {
        center: true,
        padding: {
          DEFAULT: "0.75rem",
          sm: "1rem",
        },
        screens: {
          lg: "1276px",
        },
      },
      screens: {
        sm: "640px",
        md: "960px",
        lg: "1276px",
      },
      typography: (theme: any) => ({
        DEFAULT: {
          css: {
            "--tw-prose-body": "oklch(0.523438 0.002 75)",
            "--tw-prose-headings": "oklch(0.302734 0.002 75)",
            "--tw-prose-bold": "oklch(0.302734 0.002 75)",
            "--tw-prose-invert-bold": "#ffffff",
            "--tw-prose-invert-body": "oklch(0.789063 0.002 75)",
            "--tw-prose-invert-headings": "#ffffff",
            "--tw-prose-code": "oklch(0.302734 0.002 75)",
            "--tw-prose-invert-code": "oklch(0.988281 0.002 75)",
            "--tw-prose-links": "var(--tw-prose-body)",
            "--tw-prose-invert-links": "var(--tw-prose-invert-body)",
            maxWidth: null,
            strong: {
              color: "var(--tw-prose-bold)",
              fontWeight: "500",
            },
            b: {
              color: "var(--tw-prose-bold)",
              fontWeight: "500",
            },
            a: {
              fontWeight: "400",
              textUnderlineOffset: "4px",
              color: "var(--tw-prose-links)",
              "&:hover": {
                color: "var(--color-primary-dark)",
              },
            },
            h1: {
              fontFamily: "var(--font-manrope), sans-serif",
              letterSpacing: "-0.025em",
              fontWeight: "600",
              fontSize: "1.875rem",
              lineHeight: "2.25rem",
            },
            h2: {
              fontFamily: "var(--font-manrope), sans-serif",
              letterSpacing: "-0.025em",
              fontWeight: "600",
              fontSize: "1.5rem",
              lineHeight: "2rem",
            },
            h3: {
              fontFamily: "var(--font-manrope), sans-serif",
              letterSpacing: "-0.025em",
              fontWeight: "600",
              fontSize: "1.25rem",
              lineHeight: "1.75rem",
            },
            h4: {
              fontFamily: "var(--font-manrope), sans-serif",
              letterSpacing: "-0.025em",
              fontWeight: "600",
              fontSize: "1.125rem",
              lineHeight: "1.75rem",
            },
            "code::before": {
              content: "none",
            },
            "code::after": {
              content: "none",
            },
            code: {
              backgroundColor: "oklch(0.980563 0.002 75)",
              padding: "0.15rem 0.25rem",
              borderRadius: "2px",
              whiteSpace: "nowrap",
              fontWeight: "400",
            },
            p: {
              marginBottom: "0.75rem",
              marginTop: "0.75rem",
            },
          },
        },
        xl: {
          css: {
            p: {
              marginBottom: "0.75rem",
              marginTop: "0.75rem",
            },
          },
        },
      }),
    },
  },
  plugins: [
    tailwindCSSAnimate,
    typography(),
    plugin(({ addBase }) => {
      addBase({
        "*,::before,::after,::backdrop,::file-selector-button": {
          borderColor: "var(--color-border-default)",
        },
      });
    }),
    plugin(({ addVariant }) => addVariant("label", "& :is(label)")),
    plugin(({ addUtilities }) =>
      addUtilities({
        ".text-reset, .text-reset:hover, .text-reset:focus": {
          color: "inherit",
          textDecoration: "none",
        },
      }),
    ),
  ],
};

export default config;
