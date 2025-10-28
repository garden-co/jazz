import { styled } from "goober";
import React from "react";

interface TextProps extends React.HTMLAttributes<HTMLParagraphElement> {
  muted?: boolean;
  strong?: boolean;
  small?: boolean;
  inline?: boolean;
  mono?: boolean;
}

const BaseText = React.forwardRef<HTMLParagraphElement, TextProps>(
  ({ muted, strong, small, inline, mono, ...rest }, ref) => (
    <div ref={ref} {...rest} />
  ),
);

const StyledText = styled(BaseText)<TextProps>`
  ${(props) =>
    props.muted &&
    `
    color: var(--j-neutral-500);
  `}

  ${(props) =>
    props.strong &&
    `
    font-weight: 500;
    color: var(--j-text-color-strong);
  `}

  ${(props) =>
    props.small &&
    `
    font-size: 0.875rem;
  `}

  ${(props) =>
    props.inline &&
    `
    display: inline;
  `}

  ${(props) =>
    props.mono &&
    `
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  `}
`;

export function Text(
  props: React.PropsWithChildren<{
    className?: string;
    muted?: boolean;
    strong?: boolean;
    inline?: boolean;
    small?: boolean;
    mono?: boolean;
  }>,
) {
  return <StyledText {...props} />;
}
