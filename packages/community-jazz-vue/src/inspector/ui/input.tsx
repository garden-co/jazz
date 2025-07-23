import { type PropType, computed, defineComponent } from "vue";

interface InputProps {
  label: string;
  class?: string;
  id?: string;
  hideLabel?: boolean;
  modelValue?: string;
  placeholder?: string;
  style?: string | Record<string, any>;
  type?: string;
}

export const Input = defineComponent({
  name: "Input",
  props: {
    label: {
      type: String,
      required: true,
    },
    class: String,
    id: String,
    hideLabel: {
      type: Boolean,
      default: false,
    },
    modelValue: String,
    placeholder: String,
    style: [String, Object] as PropType<string | Record<string, any>>,
    type: {
      type: String,
      default: "text",
    },
  },
  emits: {
    "update:modelValue": (value: string) => true,
  },
  setup(props, { emit, attrs }) {
    const inputId = computed(() => {
      return props.id || `input-${Math.random().toString(36).substr(2, 9)}`;
    });

    const handleInput = (event: any) => {
      const target = event.target as HTMLInputElement;
      emit("update:modelValue", target.value);
    };

    const containerStyle = {
      display: "grid",
      gap: "0.25rem",
    };

    const inputStyle = computed(() => {
      const baseStyle = {
        width: "100%",
        borderRadius: "0.375rem",
        border: "1px solid #e5e3e4",
        padding: "0.5rem 0.875rem",
        boxShadow: "0 1px 2px 0 rgba(0, 0, 0, 0.05)",
        fontWeight: "500",
        backgroundColor: "white",
        color: "#2f2e2e",
        fontFamily: "inherit",
      };

      return {
        ...baseStyle,
        ...(typeof props.style === "object" ? props.style : {}),
      };
    });

    const labelStyle = {
      position: props.hideLabel ? ("absolute" as const) : ("static" as const),
      width: props.hideLabel ? "1px" : "auto",
      height: props.hideLabel ? "1px" : "auto",
      padding: props.hideLabel ? "0" : "inherit",
      margin: props.hideLabel ? "-1px" : "inherit",
      overflow: props.hideLabel ? ("hidden" as const) : ("visible" as const),
      clip: props.hideLabel ? "rect(0, 0, 0, 0)" : "auto",
      whiteSpace: props.hideLabel ? ("nowrap" as const) : ("normal" as const),
      border: props.hideLabel ? "0" : "inherit",
    };

    return () => (
      <div style={containerStyle}>
        <label for={inputId.value} style={labelStyle}>
          {props.label}
        </label>
        <input
          {...attrs}
          id={inputId.value}
          type={props.type}
          value={props.modelValue}
          placeholder={props.placeholder}
          style={inputStyle.value}
          onInput={handleInput}
        />
      </div>
    );
  },
});
