import harmonyPalette from "@evilmartians/harmony/tailwind";
import typography from "@tailwindcss/typography";
import colors from "tailwindcss/colors";
import plugin from "tailwindcss/plugin";
import tailwindCSSAnimate from "tailwindcss-animate";
import { COLORS } from "./colors/colors";

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

const stonePaletteWithAlpha = { ...stonePalette };

Object.keys(stonePalette).forEach((key) => {
  stonePaletteWithAlpha[key] = stonePaletteWithAlpha[key].replace(
    ")",
    "/ <alpha-value>)",
  );
});

/** @type {import('tailwindcss').Config} */
export const preset = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/utils/**/*.{js,ts,jsx,tsx,mdx}",
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
          brightLight:
            "lch(from var(--color-primary) calc(l - 1) calc(c + 20) calc(h + 5))",
          brightDark:
            "lch(from var(--color-primary) calc(l - 6) calc(c + 20) calc(h + 5))",
        },
        success: {
          DEFAULT: "var(--color-success)",
          transparent: "lch(from var(--color-success) l c h / 0.3)",
          dark: "lch(from var(--color-success) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-success) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-success) calc(l - 1) calc(c + 20) calc(h + 10))",
          brightDark:
            "lch(from var(--color-success) calc(l - 6) calc(c + 20) calc(h + 10))",
        },
        info: {
          DEFAULT: "var(--color-info)",
          transparent: "lch(from var(--color-info) l c h / 0.3)",
          dark: "lch(from var(--color-info) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-info) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-info) calc(l - 1) calc(c + 20) calc(h + 5))",
          brightDark:
            "lch(from var(--color-info) calc(l - 4) calc(c + 20) calc(h + 5))",
        },
        warning: {
          DEFAULT: "var(--color-warning)",
          transparent: "lch(from var(--color-warning) l c h / 0.3)",
          dark: "lch(from var(--color-warning) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-warning) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-warning) calc(l - 1) calc(c + 30) calc(h + 15))",
          brightDark:
            "lch(from var(--color-warning) calc(l - 4) calc(c + 30) calc(h + 15))",
        },
        danger: {
          DEFAULT: "var(--color-danger)",
          transparent: "lch(from var(--color-danger) l c h / 0.3)",
          dark: "lch(from var(--color-danger) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-danger) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-danger) calc(l - 2) calc(c + 20) calc(h + 10))",
          brightDark:
            "lch(from var(--color-danger) calc(l - 6) calc(c + 10) calc(h + 10))",
        },
        tip: {
          DEFAULT: "var(--color-tip)",
          transparent: "lch(from var(--color-tip) l c h / 0.3)",
          dark: "lch(from var(--color-tip) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-tip) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-tip) calc(l - 1) calc(c + 20) calc(h + 10))",
          brightDark:
            "lch(from var(--color-tip) calc(l - 4) calc(c + 20) calc(h + 10))",
        },
        alert: {
          DEFAULT: "var(--color-alert)",
          transparent: "lch(from var(--color-alert) l c h / 0.3)",
          dark: "lch(from var(--color-alert) calc(l - 7) calc(c - 1) calc(h + 5))",
          light:
            "lch(from var(--color-alert) calc(l + 4) calc(c + 1) calc(h - 5))",
          brightLight:
            "lch(from var(--color-alert) calc(l - 1) calc(c + 50) calc(h + 15))",
          brightDark:
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
          md: "960px",
          lg: "1276px",
        },
      },
      screens: {
        md: "960px",
        lg: "1276px",
      },
      typography: (theme) => ({
        DEFAULT: {
          css: {
            "--tw-prose-body": stonePalette[700],
            "--tw-prose-headings": stonePalette[900],
            "--tw-prose-bold": stonePalette[900],
            "--tw-prose-invert-bold": theme("colors.white"),
            "--tw-prose-invert-body": stonePalette[400],
            "--tw-prose-invert-headings": theme("colors.white"),
            "--tw-prose-code": stonePalette[900],
            "--tw-prose-invert-code": stonePalette[50],
            "--tw-prose-links": theme("colors.primary"),
            "--tw-prose-invert-links": theme("colors.primary"),
            maxWidth: null,
            strong: {
              color: "var(--tw-prose-bold)",
              fontWeight: theme("fontWeight.medium"),
            },
            b: {
              color: "var(--tw-prose-bold)",
              fontWeight: theme("fontWeight.medium"),
            },
            a: {
              fontWeight: theme("fontWeight.normal"),
              textUnderlineOffset: "4px",
              "&:hover": {
                color: "var(--color-primary-dark)",
              },
            },
            h1: {
              fontFamily: theme("fontFamily.display"),
              letterSpacing: theme("letterSpacing.tight"),
              fontWeight: theme("fontWeight.semibold"),
              fontSize: theme("fontSize.3xl"),
            },
            h2: {
              fontFamily: theme("fontFamily.display"),
              letterSpacing: theme("letterSpacing.tight"),
              fontWeight: theme("fontWeight.semibold"),
              fontSize: theme("fontSize.2xl"),
            },
            h3: {
              fontFamily: theme("fontFamily.display"),
              letterSpacing: theme("letterSpacing.tight"),
              fontWeight: theme("fontWeight.semibold"),
              fontSize: theme("fontSize.xl"),
            },
            h4: {
              fontFamily: theme("fontFamily.display"),
              letterSpacing: theme("letterSpacing.tight"),
              fontWeight: theme("fontWeight.semibold"),
              fontSize: theme("fontSize.lg"),
            },
            "code::before": {
              content: "none",
            },
            "code::after": {
              content: "none",
            },
            code: {
              backgroundColor: stonePalette[100],
              padding: "0.15rem 0.25rem",
              borderRadius: "2px",
              whiteSpace: "nowrap",
              fontWeight: 400,
            },
            p: {
              marginBottom: theme("spacing.3"),
              marginTop: theme("spacing.3"),
            },
          },
        },
        xl: {
          css: {
            p: {
              marginBottom: theme("spacing.3"),
              marginTop: theme("spacing.3"),
            },
          },
        },
      }),
    },
  },
  plugins: [
    tailwindCSSAnimate,
    typography(),
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

const config = {
  presets: [preset],
  darkMode: ["class"],
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
};
export default config;
