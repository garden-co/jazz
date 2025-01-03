import fs from "fs";
import path from "path";

export const metadata = {
  title: "Upgrade guides",
  description: "How to migrate Jazz packages, and handle breaking changes.",
};

const getPaths = () => {
  const dir = path.join(process.cwd(), "components/docs/upgrades");
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  return entries.map((entry) => entry.name);
};

function UpgradeGuide({
  fileName,
  content: Content,
}: { fileName: string; content: any }) {
  const date = new Date(fileName.replace(".mdx", "")).toLocaleDateString(
    undefined,
    { year: "numeric", month: "long", day: "numeric" },
  );
  return (
    <div className="mb-10">
      <h2 className="not-prose text-sm text-stone-600 dark:text-stone-400 mb-5 pb-2 border-b">
        {date}
      </h2>
      <Content />
    </div>
  );
}

export default async function Page() {
  const paths = getPaths();
  const mdxSources = await Promise.all(
    paths.map(async (path) => {
      const { default: Content } = await import(
        `@/components/docs/upgrades/${path}`
      );
      return { path, Content };
    }),
  );

  return (
    <>
      <h1>Upgrade guides for Jazz packages</h1>

      {mdxSources.map(({ path, Content }) => (
        <UpgradeGuide fileName={path} content={Content} />
      ))}
    </>
  );
}
