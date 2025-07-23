import { computed, defineComponent } from "vue";

interface ButtonProps {
  variant?: "primary" | "secondary" | "link" | "plain";
  disabled?: boolean;
  type?: "button" | "submit" | "reset";
  onClick?: (event: Event) => void;
  style?: string | Record<string, any>;
  class?: string;
}

export const Button = defineComponent({
  name: "Button",
  props: {
    variant: {
      type: String as () => "primary" | "secondary" | "link" | "plain",
      default: "primary",
      validator: (value: string) =>
        ["primary", "secondary", "link", "plain"].includes(value),
    },
    disabled: {
      type: Boolean,
      default: false,
    },
    type: {
      type: String as () => "button" | "submit" | "reset",
      default: "button",
    },
    onClick: {
      type: Function as any,
      required: false,
    },
    style: {
      type: [String, Object] as any,
      required: false,
    },
    class: {
      type: String,
      required: false,
    },
  },
  setup(props, { slots, attrs }) {
    const buttonStyle = computed(() => {
      const baseStyle = {
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "0.5rem",
        textAlign: "center" as const,
        transition: "colors 0.2s",
        borderRadius: "0.5rem",
        border: "none",
        cursor: props.disabled ? "not-allowed" : "pointer",
        fontFamily: "inherit",
        pointerEvents: props.disabled ? ("none" as const) : ("auto" as const),
        opacity: props.disabled ? 0.5 : 1,
      };

      const variantStyles = {
        primary: {
          padding: "0.375rem 0.75rem",
          backgroundColor: "#146aff",
          color: "white",
          fontWeight: "500",
        },
        secondary: {
          padding: "0.375rem 0.75rem",
          color: "#2f2e2e",
          border: "1px solid #e5e3e4",
          fontWeight: "500",
          backgroundColor: "white",
        },
        link: {
          color: "#146aff",
          backgroundColor: "transparent",
          padding: "0",
        },
        plain: {
          backgroundColor: "transparent",
          color: "#2f2e2e",
          padding: "0.375rem 0.75rem",
        },
      };

      const currentVariantStyle =
        variantStyles[props.variant] || variantStyles.primary;

      return {
        ...baseStyle,
        ...currentVariantStyle,
        ...(typeof props.style === "object" ? props.style : {}),
      };
    });

    return () => (
      <button
        {...attrs}
        disabled={props.disabled}
        type={props.type}
        onClick={props.onClick}
        style={buttonStyle.value}
      >
        {slots.default?.()}
      </button>
    );
  },
});
