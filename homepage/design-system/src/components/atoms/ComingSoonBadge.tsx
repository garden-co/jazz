import clsx from "clsx";

export function ComingSoonBadge({ when = "soon" }: { when?: string }) {
  return (
    <span
      className={clsx(
        "bg-stone-100 dark:bg-stone-900",
        "text-stone-500 dark:text-stone-400",
        "border border-muted",
        "text-[0.6rem]",
        "px-1 py-0.5",
        "rounded-xl",
        "align-text-top",
      )}
    >
      Coming&nbsp;{when}
    </span>
  );
}
