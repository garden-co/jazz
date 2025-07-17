import { styled } from "goober";
import { defineComponent } from "vue";

interface TextProps {
  muted?: boolean;
  strong?: boolean;
  small?: boolean;
  inline?: boolean;
  mono?: boolean;
}

const BaseText = (props: TextProps & { class?: string; style?: any }) => (
  <div class={props.class} style={props.style} />
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

export const Text = defineComponent({
  name: "Text",
  props: {
    class: String,
    muted: Boolean,
    strong: Boolean,
    inline: Boolean,
    small: Boolean,
    mono: Boolean,
  },
  setup(props, { slots }) {
    return () => (
      <StyledText
        class={props.class}
        muted={props.muted}
        strong={props.strong}
        inline={props.inline}
        small={props.small}
        mono={props.mono}
      >
        {slots.default?.()}
      </StyledText>
    );
  },
});
