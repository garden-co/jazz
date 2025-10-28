import { co } from "jazz-tools";
import {
  BubbleTeaAddOnTypes,
  BubbleTeaBaseTeaTypes,
  PartialBubbleTeaOrder,
} from "./schema.ts";

type ResolveQuery = {
  addOns: true;
};

export function OrderForm({
  order,
  onSave,
  onCancel,
}: {
  order: co.loaded<typeof PartialBubbleTeaOrder, ResolveQuery>;
  onSave: (e: React.FormEvent<HTMLFormElement>) => void;
  onCancel?: () => void;
}) {
  // Handles updates to the instructions field of the order.
  // If instructions already exist, applyDiff updates them incrementally.
  // Otherwise, creates a new CoPlainText instance for the instructions.
  const handleInstructionsChange = (
    e: React.ChangeEvent<HTMLTextAreaElement>,
  ) => {
    if (order.instructions) {
      return order.instructions.$jazz.applyDiff(e.target.value);
    }
    order.$jazz.set("instructions", e.target.value);
  };

  return (
    <form onSubmit={onSave} className="grid gap-5">
      <div className="flex flex-col gap-2">
        <label htmlFor="baseTea">Base tea</label>
        <select
          name="baseTea"
          id="baseTea"
          value={order.baseTea || ""}
          className="dark:bg-transparent"
          onChange={(e) => order.$jazz.set("baseTea", e.target.value as any)}
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
            <input
              type="checkbox"
              value={addOn}
              name={addOn}
              id={addOn}
              checked={order.addOns.includes(addOn) || false}
              onChange={(e) => {
                if (e.target.checked) {
                  order.addOns.$jazz.push(addOn);
                } else {
                  order.addOns.$jazz.remove((item) => item === addOn);
                }
              }}
            />
            <label htmlFor={addOn}>{addOn}</label>
          </div>
        ))}
      </fieldset>

      <div className="flex flex-col gap-2">
        <label htmlFor="deliveryDate">Delivery date</label>
        <input
          type="date"
          name="deliveryDate"
          id="deliveryDate"
          className="dark:bg-transparent"
          value={order.deliveryDate?.toISOString().split("T")[0] || ""}
          onChange={(e) =>
            order.$jazz.set("deliveryDate", new Date(e.target.value))
          }
          required
        />
      </div>

      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          name="withMilk"
          id="withMilk"
          checked={order.withMilk}
          onChange={(e) => order.$jazz.set("withMilk", e.target.checked)}
        />
        <label htmlFor="withMilk">With milk?</label>
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="instructions">Special instructions</label>
        <textarea
          name="instructions"
          id="instructions"
          value={`${order.instructions}`}
          className="dark:bg-transparent"
          onChange={handleInstructionsChange}
        ></textarea>
      </div>
      <div className="flex gap-2 justify-end">
        {onCancel && (
          <button
            type="button"
            onClick={onCancel}
            className="bg-gray-500 hover:bg-gray-700 text-white font-bold py-2 px-4 rounded cursor-pointer"
          >
            Cancel
          </button>
        )}
        <button
          type="submit"
          className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded cursor-pointer"
        >
          Submit
        </button>
      </div>
    </form>
  );
}
