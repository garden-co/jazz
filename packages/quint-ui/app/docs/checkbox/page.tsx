import { Checkbox } from "@/src/components/checkbox";
import Link from "next/link";

export default function CheckboxPage() {
  return (
    <div>
      <h2 className="text-2xl font-bold">Checkbox</h2>
      <p>
        Checkboxes use the same intents and a subset of variants as the{" "}
        <Link href="/docs/button">Button</Link> component.
      </p>

      <h3 className="text-lg font-bold">Sizes</h3>
      <div className="flex flex-row gap-5 mb-3">
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">xs</p>
          <Checkbox sizeStyle="xs" intent="default" />
        </div>
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">sm</p>
          <Checkbox sizeStyle="sm" intent="default" />
        </div>
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">md</p>
          <Checkbox sizeStyle="md" intent="default" />
        </div>
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">lg</p>
          <Checkbox sizeStyle="lg" intent="default" />
        </div>
      </div>

      <h3 className="text-lg font-bold">Variants</h3>
      <div className="flex flex-row gap-5 mb-3">
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">default</p>
          <Checkbox variant="default" />
        </div>
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">outline</p>
          <Checkbox variant="outline" />
        </div>
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium">inverted</p>
          <Checkbox variant="inverted" />
        </div>
      </div>

      <h3 className="text-lg font-bold">Intents</h3>
      <div className="flex flex-row gap-2 mb-3">
        <Checkbox intent="default" />
        <Checkbox intent="primary" />
        <Checkbox intent="tip" />
        <Checkbox intent="success" />
        <Checkbox intent="warning" />
        <Checkbox intent="danger" />
        <Checkbox intent="info" />
        <Checkbox intent="muted" />
        <Checkbox intent="strong" />
      </div>

      <h3 className="text-lg font-bold">Intents and Variants</h3>
      <div className="flex flex-row gap-2 mb-3">
        <Checkbox intent="default" variant="outline" />
        <Checkbox intent="primary" variant="outline" />
        <Checkbox intent="tip" variant="outline" />
        <Checkbox intent="success" variant="outline" />
        <Checkbox intent="warning" variant="outline" />
        <Checkbox intent="danger" variant="outline" />
        <Checkbox intent="info" variant="outline" />
        <Checkbox intent="muted" variant="outline" />
        <Checkbox intent="strong" variant="outline" />
      </div>
      <div className="flex flex-row gap-2 mb-3">
        <Checkbox intent="default" variant="inverted" />
        <Checkbox intent="primary" variant="inverted" />
        <Checkbox intent="tip" variant="inverted" />
        <Checkbox intent="success" variant="inverted" />
        <Checkbox intent="warning" variant="inverted" />
        <Checkbox intent="danger" variant="inverted" />
        <Checkbox intent="info" variant="inverted" />
        <Checkbox intent="muted" variant="inverted" />
        <Checkbox intent="strong" variant="inverted" />
      </div>
    </div>
  );
}
