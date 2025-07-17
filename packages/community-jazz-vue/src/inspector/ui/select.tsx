import { styled } from "goober";
import { type PropType, computed, defineComponent } from "vue";
import { Icon } from "./icon.js";

interface SelectProps {
  label: string;
  hideLabel?: boolean;
  id?: string;
  class?: string;
  modelValue?: string | number;
  disabled?: boolean;
  multiple?: boolean;
  size?: number;
}

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

export const Select = defineComponent({
  name: "Select",
  props: {
    label: {
      type: String,
      required: true,
    },
    hideLabel: {
      type: Boolean,
      default: false,
    },
    id: String,
    class: String,
    modelValue: [String, Number],
    disabled: Boolean,
    multiple: Boolean,
    size: Number,
  },
  emits: ["update:modelValue", "change"],
  setup(props, { emit, slots, attrs }) {
    const selectId = computed(() => {
      return props.id || `select-${Math.random().toString(36).substr(2, 9)}`;
    });

    const handleChange = (event: any) => {
      const target = event.target as HTMLSelectElement;
      emit("update:modelValue", target.value);
      emit("change", event);
    };

    return () => (
      <SelectContainer className={props.class}>
        <label for={selectId.value} class={props.hideLabel ? "j-sr-only" : ""}>
          {props.label}
        </label>

        <SelectWrapper>
          <StyledSelect
            {...attrs}
            id={selectId.value}
            value={props.modelValue}
            disabled={props.disabled}
            multiple={props.multiple}
            size={props.size}
            onChange={handleChange}
          >
            {slots.default?.()}
          </StyledSelect>

          <SelectIcon>
            <Icon name="chevronDown" size="sm" />
          </SelectIcon>
        </SelectWrapper>
      </SelectContainer>
    );
  },
});
