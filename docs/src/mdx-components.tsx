import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";

type Status = "implemented" | "partial" | "planned" | "future";

const statusConfig: Record<Status, { label: string; color: string }> = {
  implemented: {
    label: "Implemented",
    color: "bg-green-500/20 text-green-700 dark:text-green-400",
  },
  partial: {
    label: "Partially Implemented",
    color: "bg-yellow-500/20 text-yellow-700 dark:text-yellow-400",
  },
  planned: {
    label: "Planned",
    color: "bg-blue-500/20 text-blue-700 dark:text-blue-400",
  },
  future: {
    label: "Future",
    color: "bg-gray-500/20 text-gray-700 dark:text-gray-400",
  },
};

function StatusBadge({ status }: { status: Status }) {
  const config = statusConfig[status] || statusConfig.future;
  return (
    <span
      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${config.color}`}
    >
      {config.label}
    </span>
  );
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...(defaultMdxComponents as any),
    StatusBadge,
    ...components,
  } as MDXComponents;
}
