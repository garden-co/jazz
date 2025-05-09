export function Copyright({
  className,
  companyName = "Garden Computing, Inc.",
}: {
  companyName?: string;
  className?: string;
}) {
  return (
    <p className={className}>
      © {new Date().getFullYear()} {companyName}
    </p>
  );
}
