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
      <div className="flex items-center align-middle gap-2">
        {icon && (
          <Icon
            name={icon}
            className="text-primary p-1.5 rounded-lg mb-2.5"
            size="3xl"
          />
        )}
        <div className="text-primary flex font-medium md:text-base mb-2">
          {label}
        </div>
      </div>
      {explanation && <Prose>{explanation}</Prose>}
      {children}
    </Card>
  );
}
