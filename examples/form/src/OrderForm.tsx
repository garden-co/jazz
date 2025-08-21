import { CoPlainText } from "jazz-tools";
import {
  BubbleTeaAddOnTypes,
  BubbleTeaBaseTeaTypes,
  BubbleTeaOrder,
  DraftBubbleTeaOrder,
} from "./schema.ts";
import { Button, Checkbox, Input, Label } from "quint-ui";

export function OrderForm({
  order,
  onSave,
}: {
  order: BubbleTeaOrder | DraftBubbleTeaOrder;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  // Handles updates to the instructions field of the order.
  // If instructions already exist, applyDiff updates them incrementally.
  // Otherwise, creates a new CoPlainText instance for the instructions.
  const handleInstructionsChange = (
    e: React.ChangeEvent<HTMLTextAreaElement>,
  ) => {
    if (order.instructions) {
      return order.instructions.applyDiff(e.target.value);
    }
    order.instructions = CoPlainText.create(e.target.value, order._owner);
  };

  return (
    <form onSubmit={onSave} className="grid gap-5">
      <div className="flex flex-col gap-2">
        <Label htmlFor="baseTea">Base tea</Label>
        <select
          name="baseTea"
          id="baseTea"
          value={order.baseTea || ""}
          className="dark:bg-transparent"
          onChange={(e) => (order.baseTea = e.target.value as any)}
          required
        >
          <option value="" disabled>
            Please select your preferred base tea
          </option>
          {BubbleTeaBaseTeaTypes.map((teaType) => (
            <option key={teaType} value={teaType}>
              {teaType}
            </option>
          ))}
        </select>
      </div>

      <fieldset>
        <legend className="mb-2">Add-ons</legend>

        {BubbleTeaAddOnTypes.map((addOn) => (
          <div key={addOn} className="flex items-center gap-2">
            <Checkbox
              value={addOn}
              name={addOn}
              id={addOn}
              variant="outline"
              sizeStyle="sm"
              checked={order.addOns?.includes(addOn) || false}
              onCheckedChange={(checked: boolean) => {
                if (checked) {
                  order.addOns?.push(addOn as any);
                } else {
                  order.addOns?.splice(order.addOns?.indexOf(addOn), 1);
                }
              }}
            />
            <label htmlFor={addOn}>{addOn}</label>
          </div>
        ))}
      </fieldset>

      <div className="flex flex-col gap-2">
        <Label htmlFor="deliveryDate">Delivery date</Label>
        <Input
          type="date"
          name="deliveryDate"
          id="deliveryDate"
          value={order.deliveryDate?.toISOString().split("T")[0] || ""}
          onChange={(e) => (order.deliveryDate = new Date(e.target.value))}
          required
        />
      </div>

      <div className="flex items-center gap-2">
        <Checkbox
          name="withMilk"
          id="withMilk"
          sizeStyle="sm"
          variant="outline"
          checked={order.withMilk}
          onCheckedChange={(checked) => (order.withMilk = checked)}
        />
        <Label htmlFor="withMilk">With milk?</Label>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="instructions">Special instructions</Label>
        <textarea
          name="instructions"
          id="instructions"
          value={`${order.instructions}`}
          className="dark:bg-transparent"
          onChange={handleInstructionsChange}
        ></textarea>
      </div>

      {onSave && (
        <Button type="submit" intent="info" size="lg" variant="outline">
          Submit
        </Button>
      )}
    </form>
  );
}
