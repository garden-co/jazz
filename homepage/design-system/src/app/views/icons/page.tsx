import { Icon } from "@/components/atoms/Icon";
import { Table } from "@/components/molecules/Table";

export default function IconsView() {
  return (
    <div className="p-3">
      <div className="flex gap-2">
        <Icon name="search" size="xs" intent="primary" />
        <Icon name="zip" size="md" intent="info" />
        <Icon name="docs" size="lg" intent="success" />
        <Icon name="file" size="xl" intent="warning" />
        <Icon name="hash" size="2xl" intent="danger" />
        <Icon name="help" size="3xl" intent="alert" />
        <Icon name="image" size="4xl" intent="tip" />
        <Icon name="corecord" size="5xl" intent="default" />
        <Icon name="corecord" size="6xl" intent="muted" />
        <Icon name="corecord" size="7xl" intent="strong" />
      </div>
      <div className="flex gap-2">
        <Icon name="search" size="xs" intent="primary" hasBackground />
        <Icon name="zip" size="md" intent="info" hasBackground />
        <Icon name="docs" size="lg" intent="success" hasBackground />
        <Icon name="file" size="xl" intent="warning" hasBackground />
        <Icon name="hash" size="2xl" intent="danger" hasBackground />
        <Icon name="help" size="3xl" intent="alert" hasBackground />
        <Icon name="image" size="4xl" intent="tip" hasBackground />
        <Icon name="corecord" size="5xl" intent="default" hasBackground />
        <Icon name="corecord" size="6xl" intent="muted" hasBackground />
        <Icon name="corecord" size="7xl" intent="strong" hasBackground />
      </div>
      <Table className="mt-6" tableData={iconPropsTable} copyable />
    </div>
  );
}

const iconPropsTable = {
  headers: ["prop", "types", "default"],
  data: [
    {
      prop: "name",
      types: "string",
      default: "undefined",
    },
    {
      prop: "icon",
      types: "LucideIcon",
      default: "undefined",
    },
    {
      prop: "size",
      types: ["xs", "sm", "md", "lg", "xl", "2xl", "3xl", "4xl", "5xl"],
      default: "md",
    },
    {
      prop: "intent",
      types: [
        "default",
        "primary",
        "info",
        "success",
        "warning",
        "danger",
        "alert",
        "tip",
        "muted",
        "strong",
        "white",
      ],
      default: "default",
    },
    {
      prop: "hasBackground",
      types: "boolean",
      default: "false",
    },
    {
      prop: "className",
      types: "string",
      default: "undefined",
    },
  ],
};
