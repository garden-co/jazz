import { Framework, frameworks } from "@/content/framework";
import { DocPage, generateOGMetadata, getDocMetadata } from "@/lib/docMdxContent";
import fs from "fs";
import path from "path";

type Params = {
  framework: string;
  slug?: string[];
};

type Frontmatter = {
  title: string;
  description: string;
  image?: string;
  topic?: string;
  subtopic?: string;
};

export async function generateMetadata({ params }: { params: Promise<Params> }) {
  const awaitedParams = await params;
  const framework = awaitedParams.framework;
  const slug = awaitedParams.slug ?? [];

  const docMeta = await getDocMetadata(framework, slug);

  return generateOGMetadata(framework, slug, docMeta.fm);
}

export default async function Page({ params }: { params: Promise<Params> }) {
  const awaitedParams = await params;
  const framework = awaitedParams.framework;
  const slug = awaitedParams.slug ?? [];

  return <DocPage framework={framework} slug={slug} />;
}

// --- Static Params for SSG ---
const DOCS_DIR = path.join(process.cwd(), "content", "docs");

/**
 * Recursively walks a directory and returns all MDX file paths relative to DOCS_DIR
 */
function walkDocsDir(dir: string, prefix: string[] = []): string[][] {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const paths: string[][] = [];

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      paths.push(...walkDocsDir(fullPath, [...prefix, entry.name]));
    } else if (entry.isFile() && entry.name.endsWith(".mdx")) {
      const nameWithoutExt = entry.name.replace(/\.mdx$/, "");
      paths.push([...prefix, nameWithoutExt]);
    }
  }

  return paths;
}

/**
 * Generates all static paths for Next.js
 */
function getAllDocPaths() {
  const allSlugPaths = walkDocsDir(DOCS_DIR);

  const allPaths: { framework: string; slug: string[] }[] = [];

  for (const slug of allSlugPaths) {
    // If the MDX file is already framework-specific (e.g., project-setup/react.mdx)
    const lastSegment = slug[slug.length - 1] as Framework;
    const isFrameworkSpecific = frameworks.includes(lastSegment);

    if (isFrameworkSpecific) {
      const framework = lastSegment;
      const genericSlug = slug.slice(0, -1);
      allPaths.push({ framework, slug: genericSlug });
    } else {
      // Generic MDX → replicate for all supported frameworks
      for (const framework of frameworks) {
        allPaths.push({ framework, slug });
      }
    }
  }

  // Don't generate empty slugs here - those are handled by /docs/[framework]/page.tsx
  // The required catch-all [...slug] only handles paths with at least one segment
  return allPaths.filter(path => path.slug.length > 0);
}

export async function generateStaticParams() {
  return getAllDocPaths();
}

export const dynamicParams = false;
export const dynamic = "force-static";
