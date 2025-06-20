import { Button } from "@/components/atoms/Button";
import { Table } from "@/components/molecules/Table";

export default function ButtonsPage() {
  return (
    <>
      <h3 className="text-lg mt-5 mb-2 font-bold">Variants</h3>

      <div className="grid grid-cols-2 gap-2">
        <Button>default</Button>
        <Button variant="secondary">secondary</Button>
        <Button variant="tip">tip</Button>
        <Button variant="info">info</Button>
        <Button variant="success">success</Button>
        <Button variant="warning">warning</Button>
        <Button variant="alert">alert</Button>
        <Button variant="danger">danger</Button>
        <Button variant="light">light</Button>
        <Button variant="dark">dark</Button>
        <Button variant="default">default</Button>
      </div>

      <h3 className="text-lg mt-5 mb-2 font-bold">Styles</h3>

      <div className="grid grid-cols-2 gap-2">
        <Button styleVariant="outline">outline</Button>
        <Button styleVariant="inverted">inverted</Button>
        <Button styleVariant="ghost">ghost</Button>
        <Button styleVariant="text">text</Button>
      </div>

      <p className="text-sm mt-5 mb-2">
        <strong>NB:</strong> Variants and styles are interchangeable.
      </p>

      <div className="grid grid-cols-2 gap-2">
        <Button
          styleVariant="outline"
          variant="warning"
          className="col-span-2 md:col-span-1"
        >
          outline warning
        </Button>
        <Button
          styleVariant="inverted"
          variant="success"
          className="col-span-2 md:col-span-1"
        >
          inverted success
        </Button>
        <Button
          styleVariant="ghost"
          variant="info"
          className="col-span-2 md:col-span-1"
        >
          ghost info
        </Button>
        <Button
          styleVariant="text"
          variant="tip"
          className="col-span-2 md:col-span-1"
        >
          text tip
        </Button>
      </div>
      <p>Buttons can also contain an icon.</p>

      <div className="grid grid-cols-2 gap-2">
        <Button
          icon="delete"
          styleVariant="text"
          variant="danger"
          iconPosition="right"
          className="col-span-2 md:col-span-1"
        >
          text danger with icon
        </Button>
        <Button
          icon="info"
          iconPosition="left"
          styleVariant="outline"
          variant="info"
          className="col-span-2 md:col-span-1"
        >
          outline info with icon
        </Button>
        <p className="col-span-2">
          Or just use the icon prop with any of the button variants, style
          variants and colors.
        </p>
        <Button icon="newsletter" styleVariant="text" variant="tip" />
        <Button icon="check" variant="success" />
      </div>
      <div className="overflow-auto">
        <h3 className="text-xl mt-5 mb-2 font-bold">Props Table</h3>
        <Table tableData={buttonPropsTableData} copyable={true} />
      </div>
    </>
  );
}

const buttonPropsTableData = {
  headers: ["prop", "types", "default"],
  data: [
    {
      prop: "variant?",
      types: [
        "primary",
        "secondary",
        "tip",
        "info",
        "success",
        "warning",
        "alert",
        "danger",
      ],
      default: "primary",
    },
    {
      prop: "styleVariant?",
      types: ["outline", "inverted", "ghost", "text"],
      default: "default",
    },
    {
      prop: "color?",
      types: ["light", "dark", "white", "black"],
      default: "primary",
    },
    {
      prop: "icon?",
      types: "Lucide icon name",
      default: "undefined",
    },
    {
      prop: "iconPosition?",
      types: ["left", "right"],
      default: "left",
    },
    {
      prop: "loading?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "loadingText?",
      types: "string",
      default: "Loading...",
    },
    {
      prop: "disabled?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "href?",
      types: "string",
      default: "undefined",
    },
    {
      prop: "newTab?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "size?",
      types: ["sm", "md", "lg"],
      default: "md",
    },
    {
      prop: "className?",
      types: "string",
      default: "undefined",
    },
    {
      prop: "children?",
      types: "React.ReactNode",
      default: "undefined",
    },
  ],
};
