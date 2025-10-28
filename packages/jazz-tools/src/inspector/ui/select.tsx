import { styled } from "goober";
import { useId } from "react";
import { Icon } from "./icon.js";

const SelectContainer = styled("div")<{ className?: string }>`
  display: grid;
  gap: 0.25rem;
`;

const SelectWrapper = styled("div")`
  position: relative;
  display: flex;
  align-items: center;
`;

const StyledSelect = styled("select")`
  width: 100%;
  border-radius: var(--j-radius-md);
  border: 1px solid var(--j-border-color);
  padding: 0.5rem 0.875rem 0.5rem 0.875rem;
  padding-right: 2rem;
  box-shadow: var(--j-shadow-sm);
  font-weight: 500;
  color: var(--j-text-color-strong);
  appearance: none;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-foreground);
  }
`;

const SelectIcon = styled("span")`
  position: absolute;
  right: 0.5em;
  color: var(--j-neutral-400);
  pointer-events: none;

  @media (prefers-color-scheme: dark) {
    color: var(--j-neutral-900);
  }
`;

export function Select(
  props: React.SelectHTMLAttributes<HTMLSelectElement> & {
    label: string;
    hideLabel?: boolean;
  },
) {
  const { label, hideLabel, id: customId, className, ...selectProps } = props;
  const generatedId = useId();
  const id = customId || generatedId;

  return (
    <SelectContainer className={className}>
      <label htmlFor={id} className={hideLabel ? "j-sr-only" : ""}>
        {label}
      </label>

      <SelectWrapper>
        <StyledSelect {...selectProps} id={id}>
          {props.children}
        </StyledSelect>

        <SelectIcon>
          <Icon name="chevronDown" size="sm" />
        </SelectIcon>
      </SelectWrapper>
    </SelectContainer>
  );
}
