import { styled } from "goober";
import { forwardRef, useId } from "react";

interface LabelProps {
  hideLabel?: boolean;
}

interface InputProps
  extends React.InputHTMLAttributes<HTMLInputElement>,
    LabelProps {
  // label can be hidden with a "label:sr-only" className
  label: string;
  className?: string;
  id?: string;
}

const Container = styled("div")`
  display: grid;
  gap: 0.25rem;
`;

const StyledInput = styled("input")`
  width: 100%;
  border-radius: var(--j-radius-md);
  border: 1px solid var(--j-border-color);
  padding: 0.5rem 0.875rem;
  box-shadow: var(--j-shadow-sm);
  font-weight: 500;
  background-color: white;
  color: var(--j-text-color-strong);

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-foreground);
  }
`;

export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ label, className, hideLabel, id: customId, ...inputProps }, ref) => {
    const generatedId = useId();
    const id = customId || generatedId;

    return (
      <Container className={className}>
        <label
          htmlFor={id}
          className={hideLabel ? "j-sr-only" : ""}
          style={{ color: "var(--j-text-color)" }}
        >
          {label}
        </label>
        <StyledInput ref={ref} {...inputProps} id={id} />
      </Container>
    );
  },
);
