import Input from "@/src/components/input";
import Label from "@/src/components/label";
import { SearchIcon } from "lucide-react";

export default function InputPage() {
  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-2xl mb-2 font-bold">Input</h2>
      <p className="mb-3">
        Inputs are used in conjunction with a label and can be styled with the
        intent and size props.
      </p>
      <div className="flex flex-row gap-2">
        <Label htmlFor="input">Label</Label>
        <Input id="input" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="input">Label</Label>
        <Input id="input" intent="primary" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="input" size="sm">
          Label
        </Label>
        <Input id="input" intent="tip" sizeStyle="sm" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="input" size="sm">
          Label
        </Label>
        <Input id="input" intent="info" sizeStyle="sm" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="input" size="lg">
          Label
        </Label>
        <Input id="input" intent="warning" sizeStyle="lg" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="input" size="lg">
          Label
        </Label>
        <Input id="input" intent="danger" sizeStyle="lg" />
      </div>

      <p>
        Labels should alway be used with an input, but can be hidden with the
        isHidden prop.
      </p>

      <div className="flex flex-row gap-2 items-center">
        <Label htmlFor="input" isHidden>
          Label
        </Label>
        <SearchIcon />

        <Input id="input" />
      </div>
    </div>
  );
}
