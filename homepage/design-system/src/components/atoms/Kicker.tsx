import clsx from "clsx";

export function Kicker({
  children,
  className,
  as,
}: React.ComponentPropsWithoutRef<"p"> & {
  as?: React.ElementType;
}) {
  const Element = as ?? "p";
  return (
    <Element
      className={clsx(
        className,
        "uppercase text-primary tracking-widest text-sm font-medium",
      )}
    >
      {children}
    </Element>
  );
}
