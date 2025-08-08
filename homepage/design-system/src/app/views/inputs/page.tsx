"use client";

import { useState } from "react";
import { Input } from "@/components/molecules/Input";
import { InputWithButton } from "@/components/molecules/InputWithButton";
import { Table } from "@/components/molecules/Table";
import { NewsletterForm } from "@/components/organisms/NewsletterForm";

export default function InputsView() {
  const [checked, setChecked] = useState({
    md: true,
    sm: true,
  });

  return (
    <div className="p-3">
      <p>
        Inputs consist of a combintion of atoms which can be used to create a
        variety of inputs. These atoms include:
        <br />
        <br />
        <code>Icon</code>, <code>Label</code> and <code>Button</code>, and also
        may be styled with the <code>variant</code> prop.
      </p>
      <div className="flex flex-col gap-2 mt-3">
        <h3 className="text-lg font-semibold my-2">Icons</h3>
        <Input
          icon="search"
          label="Search [label hidden]"
          iconPosition="left"
          placeholder="Search"
          labelHidden={true}
        />
        <Input
          icon="check"
          label="Email"
          iconPosition="left"
          placeholder="Email"
        />
        <Input
          icon="file"
          label="Password"
          iconPosition="right"
          placeholder="Password"
        />
        <Input
          icon="eye"
          label="Password"
          iconPosition="right"
          labelPosition="row"
          placeholder="Password"
        />
        <h3 className="text-lg font-semibold my-2">Variants</h3>
        <Input label="Muted" placeholder="Muted" intent="muted" />
        <Input label="Strong" placeholder="Strong" intent="strong" />
        <Input label="Default" placeholder="Default" intent="default" />
        <h3 className="text-lg font-semibold my-2">Buttons</h3>
        <InputWithButton
          inputProps={{
            label: "Input with button [label visible]",
            labelHidden: false,
            placeholder: "Input with button",
            intent: "success",
          }}
          buttonProps={{
            children: "Let's go",
            intent: "success",
            variant: "inverted",
            icon: "check",
            iconPosition: "left",
          }}
        />
        <InputWithButton
          inputProps={{
            label: "Input with button [label visible]",
            labelHidden: false,
            labelPosition: "row",
            placeholder: "Input with button",
          }}
          buttonProps={{
            children: "Learn more",
            intent: "tip",
            variant: "outline",
            icon: "corecord",
            iconPosition: "right",
          }}
        />
        <NewsletterForm />
      </div>
      <Table className="mt-6" tableData={inputPropsTable} copyable />
    </div>
  );
}

const inputPropsTable = {
  headers: ["prop", "types", "default"],
  data: [
    {
      prop: "label",
      types: "string",
      default: "undefined",
    },
    {
      prop: "labelHidden?",
      types: "boolean",
      default: "false",
    },
    {
      prop: "labelPosition?",
      types: ["column", "row"],
      default: "column",
    },
    {
      prop: "icon?",
      types: ["LucideIcon"],
      default: "undefined",
    },
    {
      prop: "iconPosition?",
      types: ["left", "right"],
      default: "left",
    },
    {
      prop: "intent?",
      types: [
        "primary",
        "secondary",
        "info",
        "success",
        "warning",
        "danger",
        "alert",
        "tip",
        "muted",
        "strong",
        "default",
      ],
      default: "default",
    },
    {
      prop: "buttonProps?",
      types: "see Button Props",
      default: "undefined",
    },
  ],
};
