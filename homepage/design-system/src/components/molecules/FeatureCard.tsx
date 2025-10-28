import clsx from "clsx";
import { Card } from "../atoms/Card";
import { Icon, IconName } from "../atoms/Icon";
import { Prose } from "./Prose";

export function FeatureCard({
  label,
  icon,
  explanation,
  children,
  className,
}: {
  label: React.ReactNode;
  icon?: IconName;
  explanation?: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
}) {
  return (
    <Card className={clsx(className, "p-4")}>
      <h3 className="flex items-center gap-2 md:gap-3 text-stone-950 dark:text-white font-display mb-2 font-semibold tracking-tight">
        {icon && (
          <Icon
            name={icon}
            intent="primary"
            hasBackground
            className="p-1.5"
            size="2xl"
          />
        )}
        {label}
      </h3>
      {explanation && <Prose>{explanation}</Prose>}
      {children}
    </Card>
  );
}
