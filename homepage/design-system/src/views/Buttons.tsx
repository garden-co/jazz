import { Button } from "@/components/atoms/Button";

export function Buttons() {
  return (
    <div>
      <h2 id="buttons" className="text-xl mt-5 mb-2 font-bold">
        Buttons
      </h2>

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
      </div>

      <h3 className="text-lg mt-5 mb-2 font-bold">Colors</h3>

      <div className="grid grid-cols-2 gap-2">
        <Button color="light">light</Button>
        <Button color="dark">dark</Button>
        <Button color="white">white</Button>
        <Button color="black">black</Button>
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
        <Button styleVariant="outline" variant="warning">
          outline warning
        </Button>
        <Button styleVariant="inverted" variant="success">
          inverted success
        </Button>
        <Button styleVariant="ghost" variant="info">
          ghost info
        </Button>
        <Button styleVariant="text" variant="tip">
          text tip
        </Button>
      </div>
    </div>
  );
}
