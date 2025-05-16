import { Button } from "./Button";

export function BackToHomepageContainer({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center gap-4">
      <p>{children}</p>

      <a href="/">
        <Button color="primary">Back to homepage</Button>
      </a>
    </div>
  );
}
