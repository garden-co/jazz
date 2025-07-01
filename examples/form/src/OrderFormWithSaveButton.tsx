import { useCoState } from "jazz-tools/react";
import { CoPlainText, Loaded } from "jazz-tools";
import { useEffect, useState } from "react";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import {
  BubbleTeaAddOnTypes,
  BubbleTeaBaseTeaTypes,
  BubbleTeaOrder,
  ListOfBubbleTeaAddOns,
} from "./schema.ts";

type LoadedBubbleTeaOrder = Loaded<
  typeof BubbleTeaOrder,
  { addOns: { $each: true }; instructions: true }
>;

const useOrderForm = (order: LoadedBubbleTeaOrder) => {
  const [id, setId] = useState<string | undefined>();

  const value = useCoState(BubbleTeaOrder, id, {
    resolve: { addOns: { $each: true }, instructions: true },
  });

  useEffect(() => {
    // deep clone order
    const cloned = BubbleTeaOrder.create({
      ...order,
      instructions: CoPlainText.create(`${order.instructions}` || ""),
      addOns: ListOfBubbleTeaAddOns.create([...order.addOns]),
    });
    setId(cloned.id);
  }, [order.id]);

  if (!value) return { value, save: () => {} };

  const save = () => {
    order.baseTea = value.baseTea;
    value.addOns.applyDiff(order.addOns);
    order.deliveryDate = value.deliveryDate;
    order.withMilk = value.withMilk;

    const instructions = order.instructions || CoPlainText.create("");
    if (value.instructions) {
      instructions.applyDiff(value.instructions.toString());
      order.instructions = instructions;
    }
  };

  return { value, save };
};

export function OrderFormWithSaveButton({
  order: originalOrder,
}: {
  order: LoadedBubbleTeaOrder;
}) {
  const { value: order, save } = useOrderForm(originalOrder);

  if (!order) return null;

  const handleInstructionsChange = (
    e: React.ChangeEvent<HTMLTextAreaElement>,
  ) => {
    if (order.instructions) {
      return order.instructions.applyDiff(e.target.value);
    }
    order.instructions = CoPlainText.create(e.target.value, order._owner);
  };

  const submit = (e: React.FormEvent<HTMLFormElement>) => {
    console.log("submit form");
    e.preventDefault();
    save();
  };

  return (
    <form onSubmit={submit} className="grid gap-5">
      <div>
        <p>Unsaved order:</p>
        <OrderThumbnail order={order} />
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="baseTea">Base tea</label>
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
            <input
              type="checkbox"
              value={addOn}
              name={addOn}
              id={addOn}
              checked={order.addOns?.includes(addOn) || false}
              onChange={(e) => {
                if (e.target.checked) {
                  order.addOns?.push(addOn);
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
        <label htmlFor="deliveryDate">Delivery date</label>
        <input
          type="date"
          name="deliveryDate"
          id="deliveryDate"
          className="dark:bg-transparent"
          value={order.deliveryDate?.toISOString().split("T")[0] || ""}
          onChange={(e) => (order.deliveryDate = new Date(e.target.value))}
          required
        />
      </div>

      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          name="withMilk"
          id="withMilk"
          checked={order.withMilk}
          onChange={(e) => (order.withMilk = e.target.checked)}
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

      <button
        type="submit"
        className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
      >
        Submit
      </button>
    </form>
  );
}
