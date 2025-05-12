import clsx from "clsx";

type HeadingProps = {
  level?: 1 | 2 | 3 | 4 | 5 | 6;
  size?: 1 | 2 | 3 | 4 | 5 | 6;
} & React.ComponentPropsWithoutRef<"h1" | "h2" | "h3" | "h4" | "h5" | "h6">;

const classes = {
  1: ["text-5xl lg:text-6xl", "mb-3", "font-medium", "tracking-tighter"],
  2: ["text-2xl md:text-4xl", "mb-2", "font-semibold", "tracking-tight"],
  3: ["text-xl md:text-2xl", "mb-2", "font-semibold", "tracking-tight"],
  4: ["text-bold"],
  5: [],
  6: [],
};

export function Heading({
  className,
  level = 1,
  size: customSize,
  ...props
}: HeadingProps) {
  let Element: `h${typeof level}` = `h${level}`;
  const size = customSize || level;

  return (
    <Element
      {...props}
      className={clsx(
        "text-stone-950 dark:text-white font-display",
        classes[size],
      )}
    />
  );
}
