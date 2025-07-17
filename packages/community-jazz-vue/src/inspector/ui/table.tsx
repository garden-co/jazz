import { styled } from "goober";
import { defineComponent } from "vue";

const StyledTable = styled("table")`
  width: 100%;
`;

const StyledThead = styled("thead")`
  text-align: left;
  border-bottom: 1px solid var(--j-border-color);
  background-color: var(--j-neutral-100);

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-neutral-925);
  }
`;

const StyledTbody = styled("tbody")`
  tr {
    border-bottom: 1px solid var(--j-border-color);

    &:last-child {
      border-bottom: none;
    }
  }
`;

const StyledTh = styled("th")`
  font-weight: 500;
  padding: 0.5rem 0.75rem;
  color: var(--j-text-color-strong);
`;

const StyledTd = styled("td")`
  padding: 0.5rem 0.75rem;
`;

export const Table = defineComponent({
  name: "Table",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <StyledTable
        {...attrs}
        className={props.class}
        style={props.style as any}
      >
        {slots.default?.()}
      </StyledTable>
    );
  },
});

export const TableHead = defineComponent({
  name: "TableHead",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <StyledThead
        {...attrs}
        className={props.class}
        style={props.style as any}
      >
        {slots.default?.()}
      </StyledThead>
    );
  },
});

export const TableBody = defineComponent({
  name: "TableBody",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <StyledTbody
        {...attrs}
        className={props.class}
        style={props.style as any}
      >
        {slots.default?.()}
      </StyledTbody>
    );
  },
});

export const TableRow = defineComponent({
  name: "TableRow",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <tr {...attrs} class={props.class} style={props.style as any}>
        {slots.default?.()}
      </tr>
    );
  },
});

export const TableHeader = defineComponent({
  name: "TableHeader",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <StyledTh {...attrs} className={props.class} style={props.style as any}>
        {slots.default?.()}
      </StyledTh>
    );
  },
});

export const TableCell = defineComponent({
  name: "TableCell",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    return () => (
      <StyledTd {...attrs} className={props.class} style={props.style as any}>
        {slots.default?.()}
      </StyledTd>
    );
  },
});
