import { computed, defineComponent } from "vue";

export const Heading = defineComponent({
  name: "Heading",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots }) {
    const headingStyle = computed(() => {
      const baseStyle = {
        fontSize: "1.125rem",
        textAlign: "center" as const,
        fontWeight: "500",
        color: "#2f2e2e",
        margin: "0",
      };

      return {
        ...baseStyle,
        ...(typeof props.style === "object" ? props.style : {}),
      };
    });

    return () => <h1 style={headingStyle.value}>{slots.default?.()}</h1>;
  },
});
