import { classNames } from "../utils.js";

export function Badge({
  children,
  className,
}: React.PropsWithChildren<{ className?: string }>) {
  return (
    <span
      className={classNames(
        "text-sm text-gray-700 font-medium py-0.5 px-1 -ml-0.5 rounded bg-gray-700/5 inline-block font-mono",
        className,
      )}
    >
      {children}
    </span>
  );
}
