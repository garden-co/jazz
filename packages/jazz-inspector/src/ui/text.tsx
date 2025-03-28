import { classNames } from "../utils.js";

export function Text({
  children,
  className,
  muted,
  strong,
  inline,
  small,
}: React.PropsWithChildren<{
  className?: string;
  muted?: boolean;
  strong?: boolean;
  inline?: boolean;
  small?: boolean;
}>) {
  const classes = {
    muted: "text-gray-500 dark:text-gray-400",
    strong: "font-medium text-gray-900 dark:text-white",
  };

  const Element = inline ? "span" : "p";

  return (
    <Element
      className={classNames(
        muted ? classes.muted : strong ? classes.strong : null,
        {
          "text-sm": small,
        },
        className,
      )}
    >
      {children}
    </Element>
  );
}
