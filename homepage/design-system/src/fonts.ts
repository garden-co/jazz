import localFont from "next/font/local";

const manrope = localFont({
  src: [
    {
      path: "../fonts/Manrope-Bold.woff2",
      weight: "700",
      style: "normal",
    },
    {
      path: "../fonts/Manrope-Bold.woff",
      weight: "700",
      style: "normal",
    },
    {
      path: "../fonts/Manrope-Medium.woff2",
      weight: "500",
      style: "normal",
    },
    {
      path: "../fonts/Manrope-Medium.woff",
      weight: "500",
      style: "normal",
    },
    {
      path: "../fonts/Manrope-SemiBold.woff2",
      weight: "600",
      style: "normal",
    },
    {
      path: "../fonts/Manrope-SemiBold.woff",
      weight: "600",
      style: "normal",
    },
  ],
  variable: "--font-manrope",
  display: "swap",
});

const inter = localFont({
  src: [
    {
      path: "../fonts/Inter-Medium.woff2",
      weight: "500",
      style: "normal",
    },
    {
      path: "../fonts/Inter-Medium.woff",
      weight: "500",
      style: "normal",
    },
    {
      path: "../fonts/Inter-Regular.woff2",
      weight: "400",
      style: "normal",
    },
    {
      path: "../fonts/Inter-Regular.woff",
      weight: "400",
      style: "normal",
    },
    {
      path: "../fonts/Inter-SemiBold.woff2",
      weight: "600",
      style: "normal",
    },
    {
      path: "../fonts/Inter-SemiBold.woff",
      weight: "600",
      style: "normal",
    },
  ],
  variable: "--font-inter",
  display: "swap",
});

const commitMono = localFont({
  src: [
    {
      path: "../fonts/CommitMono-Regular.woff2",
      weight: "400",
      style: "normal",
    },
    {
      path: "../fonts/CommitMono-Regular.woff",
      weight: "400",
      style: "normal",
    },
  ],
  variable: "--font-commit-mono",
  display: "swap",
});

export const fontClasses = [
  manrope.variable,
  commitMono.variable,
  inter.className,
];
