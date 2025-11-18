// mdx-processor.mjs
import { promises as fs } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { bundleMDX } from "mdx-bundler";
import { getMDXComponent } from "mdx-bundler/client/index.js";
import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { NodeHtmlMarkdown } from "node-html-markdown";
import * as mockNextNavigation from "./mock-next-navigation.mjs";
import { replaceCodeSnippets } from "./replace-code-snippets.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CWD = process.cwd();

const htmlToMarkdown = new NodeHtmlMarkdown();

const mockMdxComponentsPath = path.resolve(__dirname, "mock-mdx-components.mjs");

export async function mdxToMd(filePath, framework) {
  let source = await fs.readFile(filePath, "utf-8");
  
  // Replace code snippet references with actual code content
  source = replaceCodeSnippets(source, filePath);
  
  const mockComponentsContent = await fs.readFile(mockMdxComponentsPath, "utf-8");

  // We need to pass these to esbuild
  const snippetsDir = path.join(CWD, "components/docs/snippets");
  const snippetFilenames = (await fs.readdir(snippetsDir)).filter((file) =>
    file.endsWith(".mdx")
  );

  // Read all snippet files concurrently
  const snippetEntries = await Promise.all(
    snippetFilenames.map(async (name) => {
      const content = await fs.readFile(path.join(snippetsDir, name), "utf-8");
      const importPath = `@/components/docs/snippets/${name}`;
      return [importPath, content];
    })
  );

  const { code } = await bundleMDX({
    source,
    cwd: path.dirname(filePath),
    files: {
      // Mock all the components
      "@/components/forMdx": mockComponentsContent, 
      // Adds all snippet files for bundling
      ...Object.fromEntries(snippetEntries),
    },
    esbuildOptions: (options) => {
      options.platform = "node";
      options.external = [...(options.external || []), "next/navigation"];
      options.alias = {
        ...(options.alias || {}),
        "@/components/forMdx": mockMdxComponentsPath,
      };
      // Define CURRENT_FRAMEWORK as a build-time constant
      options.define = {
        ...(options.define || {}),
        'CURRENT_FRAMEWORK': framework ? `"${framework}"` : 'null',
      };
      return options;
    },
    globals: {
      "next/navigation": {
        varName: "nextNavigation",
        namedExports: ["useParams", "usePathname", "useRouter", "useSearchParams"],
      },
    },
  });

  // Some of our components rely on router exports
  const Component = getMDXComponent(code, {
    nextNavigation: mockNextNavigation,
  });
  
  const element = createElement(Component);
  const html = renderToString(element);

  return htmlToMarkdown.translate(html);
}