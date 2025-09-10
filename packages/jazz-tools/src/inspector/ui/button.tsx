import { styled } from "goober";
import { forwardRef } from "react";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "link" | "plain" | "destructive";
  children?: React.ReactNode;
  className?: string;
  disabled?: boolean;
}

const StyledButton = styled("button")<{ variant: string; disabled?: boolean }>`
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  text-align: center;
  transition: colors 0.2s;
  border-radius: var(--j-radius-lg);
  pointer-events: ${(props) => (props.disabled ? "none" : "auto")};
  opacity: ${(props) => (props.disabled ? 0.5 : 1)};
  cursor: ${(props) => (props.disabled ? "not-allowed" : "pointer")};

  ${(props) => {
    switch (props.variant) {
      case "primary":
        return `
          padding: 0.375rem 0.75rem;
          background-color: var(--j-primary-color);
          border-color: var(--j-primary-color);
          color: white;
          font-weight: 500;
          &:hover {
            opacity: 0.8;
          }
        `;
      case "secondary":
        return `
          padding: 0.375rem 0.75rem;
          color: var(--j-text-color-strong);
          border: 1px solid var(--j-border-color);
          font-weight: 500;
          &:hover {
            border-color: var(--j-border-color-hover);
          }
        `;
      case "link":
        return `
          color: var(--j-link-color);
          &:hover {
            text-decoration: underline;
          }
        `;
      case "destructive":
        return `
          padding: 0.375rem 0.75rem;
          background-color: var(--j-destructive-color);
          border-color: var(--j-destructive-color);
          color: white;
          font-weight: 500;
          &:hover {
            opacity: 0.8;
          }
        `;
      default:
        return "";
    }
  }}
`;

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      variant = "primary",
      disabled,
      type = "button",
      ...buttonProps
    },
    ref,
  ) => {
    return (
      <StyledButton
        ref={ref}
        {...buttonProps}
        disabled={disabled}
        className={className}
        type={type}
        variant={variant}
      >
        {children}
      </StyledButton>
    );
  },
);
