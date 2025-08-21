import { Input, Label } from "quint-ui";
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
        <Label htmlFor="default">Label</Label>
        <Input id="default" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="primary">Label</Label>
        <Input id="primary" intent="primary" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="tip" size="sm">
          Label
        </Label>
        <Input id="tip" intent="tip" sizeStyle="sm" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="info" size="sm">
          Label
        </Label>
        <Input id="info" intent="info" sizeStyle="sm" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="warning" size="lg">
          Label
        </Label>
        <Input id="warning" intent="warning" sizeStyle="lg" />
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="danger" size="lg">
          Label
        </Label>
        <Input id="danger" intent="danger" sizeStyle="lg" />
      </div>

      <p>
        Labels should alway be used with an input, but can be hidden with the
        isHiddenVisually prop.
      </p>

      <div className="flex flex-row gap-2 items-center">
        <Label htmlFor="hidden" isHiddenVisually>
          Label
        </Label>
        <SearchIcon />

        <Input id="hidden" />
      </div>

      <p>
        All types of inputs are supported, including file, number, and date.
      </p>

      <div className="flex flex-row gap-2">
        <Label htmlFor="email">Email</Label>
        <Input id="email" type="email" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="color">Color</Label>
        <Input id="color" type="color" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="password">Password</Label>
        <Input id="password" type="password" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="search">Search</Label>
        <Input id="search" type="search" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="file">File</Label>
        <Input id="file" type="file" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="number">Number</Label>
        <Input id="number" type="number" />
      </div>

      <div className="flex flex-row gap-2">
        <Label htmlFor="date">Date</Label>
        <Input id="date" type="date" />
      </div>
    </div>
  );
}
