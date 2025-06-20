"use client";

import { Input } from "@/components/molecules/Input";
import { InputWithButton } from "@/components/molecules/InputWithButton";
import { Table } from "@/components/molecules/Table";
import { NewsletterForm } from "@/components/organisms/NewsletterForm";
import { useState } from "react";

export default function InputsView() {
  const [checked, setChecked] = useState({
    md: true,
    sm: true,
  });

  return (
    <div className="p-3">
      <h3 className="text-md font-semibold mb-2">Inputs</h3>
      <div className="flex flex-col gap-2">
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
        <InputWithButton
          inputProps={{
            label: "Input with button [label visible]",
            labelHidden: false,
            placeholder: "Input with button",
            variant: "success",
          }}
          buttonProps={{
            children: "Let's go",
            variant: "success",
            styleVariant: "inverted",
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
            variant: "tip",
            styleVariant: "outline",
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
      prop: "labelHidden",
      types: "boolean",
      default: "false",
    },
    {
      prop: "labelPosition",
      types: ["column", "row"],
      default: "column",
    },
    {
      prop: "icon",
      types: ["LucideIcon"],
      default: "undefined",
    },
    {
      prop: "iconPosition",
      types: ["left", "right"],
      default: "left",
    },
    {
      prop: "buttonProps",
      types: "see Button Props",
      default: "undefined",
    },
  ],
};
