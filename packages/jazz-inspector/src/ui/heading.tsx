import { classNames } from "../utils.js";

export function Heading({
  children,
  className,
}: React.PropsWithChildren<{ className?: string }>) {
  return (
    <h1
      className={classNames(
        "text-lg text-center font-medium text-stone-900 dark:text-white",
        className,
      )}
    >
      {children}
    </h1>
  );
}
