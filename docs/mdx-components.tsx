import defaultMdxComponents from "fumadocs-ui/mdx";
import * as TabsComponents from "./components/mdx/tabs";
import type { MDXComponents } from "mdx/types";
import type { ReactNode } from "react";
import { Mermaid } from "./components/mdx/mermaid";
import { GenerateAppId } from "./components/mdx/generate-app-id";
import { CloudConfig } from "./components/mdx/cloud-config";
import { DeployCommand } from "./components/mdx/deploy-command";
import { LensDiagram } from "./components/mdx/lens-diagram";
import { JazzLogo } from "./components/brand/jazz-logo";

function SlideCodeCell({ children, title }: { children: ReactNode; title: string }) {
  return (
    <div className="relative flex min-h-0 flex-col [&_.line]:whitespace-pre [&_button]:hidden [&_code]:whitespace-pre [&_figure]:my-0 [&_figure]:flex [&_figure]:h-full [&_figure]:flex-col [&_figure]:rounded-[0.8vw] [&_figure]:border-black/10 [&_figure]:bg-white/75 [&_figure]:text-[0.68vw] [&_figure]:shadow-none [&_figure>div]:max-h-none [&_figure>div]:flex-1 [&_figure>div]:py-[0.8vw] [&_figure>div]:text-[1.2vw] [&_figure>div]:leading-[1.1] [&_figure>div]:tracking-[-0.015em] [&_pre]:whitespace-pre">
      <h3 className="absolute bottom-[0.3vw] right-[0.7vw] z-10 mb-[0.1vw] text-[1vw] font-bold uppercase tracking-[0.14em] text-black/80">
        {title}
      </h3>
      {children}
    </div>
  );
}

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    ...TabsComponents,
    Mermaid,
    GenerateAppId,
    CloudConfig,
    DeployCommand,
    LensDiagram,
    JazzLogo,
    SlideCodeCell,
    ...components,
  };
}
