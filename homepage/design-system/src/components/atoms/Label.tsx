import { Label as LabelRadix } from "radix-ui";

export function Label({
  label,
  htmlFor,
  className,
}: { label: string; htmlFor: string; className?: string }) {
  return (
    <LabelRadix.Root className={className} htmlFor={htmlFor}>
      {label}
    </LabelRadix.Root>
  );
}
