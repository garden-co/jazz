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

function Heading({
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

export function H1(
  props: React.ComponentPropsWithoutRef<"h1"> & React.PropsWithChildren,
) {
  return <Heading level={1} {...props} />;
}

export function H2(
  props: React.ComponentPropsWithoutRef<"h2"> & React.PropsWithChildren,
) {
  return <Heading level={2} {...props} />;
}

export function H3(
  props: React.ComponentPropsWithoutRef<"h3"> & React.PropsWithChildren,
) {
  return <Heading level={3} {...props} />;
}

export function H4(
  props: React.ComponentPropsWithoutRef<"h4"> & React.PropsWithChildren,
) {
  return <Heading level={4} {...props} />;
}

export function H5(
  props: React.ComponentPropsWithoutRef<"h5"> & React.PropsWithChildren,
) {
  return <Heading level={5} {...props} />;
}

export function H6(
  props: React.ComponentPropsWithoutRef<"h6"> & React.PropsWithChildren,
) {
  return <Heading level={6} {...props} />;
}
