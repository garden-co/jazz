import { styled } from "goober";

export const GlobalStyles = styled("div")`
  /* Colors */
  --j-primary-color: #146AFF;
  --j-link-color: var(--j-primary-color);
  --j-success-color: oklch(52.7% 0.154 150.069);
  --j-destructive-color: oklch(50.5% 0.213 27.518);
  
  /* Neutral Colors */
  --j-neutral-100: #faf8f8;
  --j-neutral-200: #e5e3e4;
  --j-neutral-300: #d0cecf;
  --j-neutral-400: #bbbaba;
  --j-neutral-500: #a8a6a6;
  --j-neutral-600: #858484;
  --j-neutral-700: #6b696a;
  --j-neutral-900: #2f2e2e;
  --j-neutral-925: #1b1a1a;
  --j-neutral-950: #151414;
  
  /* Text Colors */
  --j-text-color: var(--j-neutral-700);
  --j-text-color-strong: var(--j-neutral-900);

  /* Border Colors */
  --j-border-color: var(--j-neutral-200);
  --j-border-color-hover: var(--j-neutral-300);
  --j-border-dark: var(--j-neutral-900);
  --j-border-focus: var(--j-primary-color);

  /* Background Colors */
  --j-background: #FFFFFF;
  --j-foreground: var(--j-neutral-100);

  /* Border Radius */
  --j-radius-sm: 0.25rem;
  --j-radius-md: 0.375rem;
  --j-radius-lg: 0.5rem;

  /* Shadows */
  --j-shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);

  @media (prefers-color-scheme: dark) {
    --j-text-color: var(--j-neutral-400);
    --j-border-color: var(--j-neutral-900);
    --j-background: var(--j-neutral-950);
    --j-foreground: var(--j-neutral-925);
    --j-border-color-hover: var(--j-neutral-700);
    --j-text-color-strong: var(--j-neutral-100);
    --j-success-color: oklch(72.3% 0.219 149.579);
    --j-destructive-color: oklch(63.7% 0.237 25.331);
  }
  
  *:focus {
    outline: none;
  }
  
  *:focus-visible {
    box-shadow: 0 0 0 2px var(--j-link-color);
  }
  
  .j-sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }
`;
