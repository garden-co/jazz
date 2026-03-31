"use client";

import { use, useEffect, useId, useState } from "react";
import { useTheme } from "next-themes";
import type { MermaidConfig } from "mermaid";

const lightThemeVariables = {
  fontFamily: "Inter, system-ui, sans-serif",
  fontSize: "14px",
  primaryColor: "hsl(220, 60%, 93%)",
  primaryTextColor: "hsl(220, 30%, 12%)",
  primaryBorderColor: "hsl(220, 20%, 78%)",
  lineColor: "hsl(220, 15%, 70%)",
  secondaryColor: "hsl(220, 30%, 96%)",
  secondaryTextColor: "hsl(220, 30%, 12%)",
  secondaryBorderColor: "hsl(220, 20%, 85%)",
  tertiaryColor: "hsl(220, 20%, 97%)",
  tertiaryTextColor: "hsl(220, 30%, 12%)",
  tertiaryBorderColor: "hsl(220, 20%, 88%)",
  background: "#fffefc",
  mainBkg: "hsl(220, 60%, 93%)",
  nodeBorder: "hsl(220, 20%, 78%)",
  clusterBkg: "hsl(220, 20%, 97%)",
  clusterBorder: "hsl(220, 20%, 88%)",
  titleColor: "hsl(220, 30%, 12%)",
  edgeLabelBackground: "#fffefc",
  textColor: "hsl(220, 30%, 12%)",
  nodeTextColor: "hsl(220, 30%, 12%)",
  actorTextColor: "hsl(220, 30%, 25%)",
  labelTextColor: "hsl(220, 15%, 45%)",
  loopTextColor: "hsl(220, 15%, 45%)",
  noteBkgColor: "hsl(220, 30%, 96%)",
  noteTextColor: "hsl(220, 30%, 25%)",
  noteBorderColor: "hsl(220, 20%, 85%)",
  actorBkg: "hsl(220, 60%, 93%)",
  actorBorder: "hsl(220, 20%, 78%)",
  activationBkgColor: "hsl(220, 30%, 96%)",
  activationBorderColor: "hsl(220, 20%, 78%)",
  signalColor: "hsl(220, 15%, 45%)",
  signalTextColor: "hsl(220, 30%, 25%)",
};

const darkThemeVariables = {
  fontFamily: "Inter, system-ui, sans-serif",
  fontSize: "14px",
  primaryColor: "hsl(220, 30%, 18%)",
  primaryTextColor: "hsl(220, 15%, 88%)",
  primaryBorderColor: "hsl(220, 25%, 28%)",
  lineColor: "hsl(220, 15%, 40%)",
  secondaryColor: "hsl(220, 25%, 14%)",
  secondaryTextColor: "hsl(220, 15%, 88%)",
  secondaryBorderColor: "hsl(220, 25%, 24%)",
  tertiaryColor: "hsl(220, 30%, 12%)",
  tertiaryTextColor: "hsl(220, 15%, 88%)",
  tertiaryBorderColor: "hsl(220, 25%, 22%)",
  background: "hsl(220, 40%, 8%)",
  mainBkg: "hsl(220, 30%, 18%)",
  nodeBorder: "hsl(220, 25%, 28%)",
  clusterBkg: "hsl(220, 30%, 12%)",
  clusterBorder: "hsl(220, 25%, 22%)",
  titleColor: "hsl(220, 15%, 88%)",
  edgeLabelBackground: "hsl(220, 40%, 8%)",
  textColor: "hsl(220, 15%, 88%)",
  nodeTextColor: "hsl(220, 15%, 88%)",
  actorTextColor: "hsl(220, 15%, 80%)",
  labelTextColor: "hsl(220, 15%, 55%)",
  loopTextColor: "hsl(220, 15%, 55%)",
  noteBkgColor: "hsl(220, 25%, 14%)",
  noteTextColor: "hsl(220, 15%, 80%)",
  noteBorderColor: "hsl(220, 25%, 24%)",
  actorBkg: "hsl(220, 30%, 18%)",
  actorBorder: "hsl(220, 25%, 28%)",
  activationBkgColor: "hsl(220, 25%, 14%)",
  activationBorderColor: "hsl(220, 25%, 28%)",
  signalColor: "hsl(220, 15%, 55%)",
  signalTextColor: "hsl(220, 15%, 80%)",
};

function getConfig(isDark: boolean): MermaidConfig {
  return {
    startOnLoad: false,
    securityLevel: "loose",
    fontFamily: "Inter, system-ui, sans-serif",
    theme: "base",
    themeVariables: isDark ? darkThemeVariables : lightThemeVariables,
    flowchart: {
      curve: "linear",
      padding: 16,
      nodeSpacing: 40,
      rankSpacing: 50,
    },
    themeCSS: `
      .node rect, .node polygon, .node circle { stroke-width: 1px; }
      .edgePath path.path { stroke-width: 1px; }
      marker#arrowhead path { fill: ${isDark ? "hsl(220, 15%, 40%)" : "hsl(220, 15%, 70%)"}; }
    `,
  };
}

export function Mermaid({ chart }: { chart: string }) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) return null;
  return <MermaidContent chart={chart} />;
}

const cache = new Map<string, Promise<unknown>>();

function cachePromise<T>(key: string, setPromise: () => Promise<T>): Promise<T> {
  const cached = cache.get(key);
  if (cached) return cached as Promise<T>;

  const promise = setPromise();
  cache.set(key, promise);
  return promise;
}

function MermaidContent({ chart }: { chart: string }) {
  const id = useId();
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === "dark";
  const { default: mermaid } = use(cachePromise("mermaid", () => import("mermaid")));

  mermaid.initialize(getConfig(isDark));

  const { svg, bindFunctions } = use(
    cachePromise(`${chart}-${resolvedTheme}`, () => {
      return mermaid.render(id, chart.replaceAll("\\n", "\n"));
    }),
  );

  return (
    <div
      ref={(container) => {
        if (container) bindFunctions?.(container);
      }}
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
