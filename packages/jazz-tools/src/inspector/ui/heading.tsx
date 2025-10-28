import { styled } from "goober";

const StyledHeading = styled("h1")<{ className?: string }>`
  font-size: 1.125rem;
  text-align: center;
  font-weight: 500;
  color: var(--j-text-color-strong);
`;

export function Heading({
  children,
  className,
  id,
}: React.PropsWithChildren<{ className?: string; id?: string }>) {
  return (
    <StyledHeading className={className} id={id}>
      {children}
    </StyledHeading>
  );
}
