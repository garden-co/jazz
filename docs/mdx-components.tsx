import defaultMdxComponents from "fumadocs-ui/mdx";
import * as TabsComponents from "fumadocs-ui/components/tabs";
import type { MDXComponents } from "mdx/types";
import { Mermaid } from "./components/mdx/mermaid";
import { GenerateAppId } from "./components/mdx/generate-app-id";
import { CloudConfig } from "./components/mdx/cloud-config";
import { DeployCommand } from "./components/mdx/deploy-command";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    ...TabsComponents,
    Mermaid,
    GenerateAppId,
    CloudConfig,
    DeployCommand,
    ...components,
  };
}
