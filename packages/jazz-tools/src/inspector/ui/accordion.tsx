import { styled } from "goober";
import { PropsWithChildren, useEffect, useState } from "react";

type AccordionProps = PropsWithChildren<{
  title: string;
  storageKey: string;
}>;

export function Accordion({ title, children, storageKey }: AccordionProps) {
  const [open, setOpen] = useStoragedState(storageKey, false);

  return (
    <details
      open={open}
      style={{ display: "flex", flexDirection: "column", gap: "1rem" }}
    >
      <StyledSummary
        onClick={(e) => {
          e.preventDefault();
          setOpen((v) => !v);
        }}
      >
        {title}
      </StyledSummary>
      {children}
    </details>
  );
}

function useStoragedState<T>(
  key: string,
  defaultValue: T,
): [T, React.Dispatch<React.SetStateAction<T>>] {
  const [state, setState] = useState<T>(() => {
    if (typeof window === "undefined") return defaultValue;
    const stored = localStorage.getItem(key);
    return stored ? JSON.parse(stored) : defaultValue;
  });

  useEffect(() => {
    localStorage.setItem(key, JSON.stringify(state));
  }, [state]);

  return [state, setState];
}

const StyledSummary = styled("summary")`
  font-size: 1.125rem;
  cursor: pointer;
  font-weight: 500;
  color: var(--j-text-color-strong);
`;
