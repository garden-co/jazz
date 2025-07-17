import { computed, defineComponent } from "vue";

export const Badge = defineComponent({
  name: "Badge",
  props: {
    class: String,
  },
  setup(props, { slots }) {
    const badgeClasses = computed(() => {
      const classes = ["jazz-badge"];
      if (props.class) {
        classes.push(props.class);
      }
      return classes.join(" ");
    });

    return () => <span class={badgeClasses.value}>{slots.default?.()}</span>;
  },
});
