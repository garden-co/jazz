import { clsx } from "clsx";

export function Alert({
  children,
  variant = "danger",
}: {
  children: React.ReactNode;
  variant?: "danger" | "success";
}) {
  const styles = {
    danger:
      "border-red-400 dark:border-red-500 bg-red-50 dark:bg-red-200/5 text-red-700 dark:text-red-400",
    success:
      "border-green-400 dark:border-green-500 bg-green-50 dark:bg-green-200/5 text-green-700 dark:text-green-400",
  };

  const style = styles[variant];

  return (
    <div className={clsx("border-l-4 p-4", style)}>
      <p className="ml-2 text-sm">{children}</p>
    </div>
  );
}
