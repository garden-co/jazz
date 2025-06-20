import { Icon } from "@/components/atoms/Icon";
import { Table } from "@/components/molecules/Table";

export default function IconsView() {
  return (
    <div className="p-3">
      <div className="flex gap-2">
        <Icon name="search" size="xs" variant="primary" />
        <Icon name="upload" size="sm" variant="secondary" />
        <Icon name="zip" size="md" variant="info" />
        <Icon name="docs" size="lg" variant="success" />
        <Icon name="file" size="xl" variant="warning" />
        <Icon name="hash" size="2xl" variant="danger" />
        <Icon name="help" size="3xl" variant="alert" />
        <Icon name="image" size="4xl" variant="tip" />
        <Icon name="corecord" size="5xl" variant="default" />
        <Icon name="corecord" size="6xl" variant="light" />
        <Icon name="corecord" size="7xl" variant="dark" />
      </div>
      <div className="flex gap-2">
        <Icon name="search" size="xs" variant="primary" hasBackground />
        <Icon name="upload" size="sm" variant="secondary" hasBackground />
        <Icon name="zip" size="md" variant="info" hasBackground />
        <Icon name="docs" size="lg" variant="success" hasBackground />
        <Icon name="file" size="xl" variant="warning" hasBackground />
        <Icon name="hash" size="2xl" variant="danger" hasBackground />
        <Icon name="help" size="3xl" variant="alert" hasBackground />
        <Icon name="image" size="4xl" variant="tip" hasBackground />
        <Icon name="corecord" size="5xl" variant="default" hasBackground />
        <Icon name="corecord" size="6xl" variant="light" hasBackground />
        <Icon name="corecord" size="7xl" variant="dark" hasBackground />
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
      prop: "variant",
      types: [
        "default",
        "primary",
        "secondary",
        "info",
        "success",
        "warning",
        "danger",
        "alert",
        "tip",
        "white",
        "black",
        "light",
        "dark",
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
