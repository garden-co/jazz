import { CoPlainText, Loaded } from "jazz-tools";
import { useEffect, useState } from "react";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import {
  BubbleTeaAddOnTypes,
  BubbleTeaBaseTeaTypes,
  BubbleTeaOrder,
  ListOfBubbleTeaAddOns,
} from "./schema.ts";

const useOrderForm = (
  order: Loaded<
    typeof BubbleTeaOrder,
    { addOns: { $each: true }; instructions: true }
  >,
) => {
  const [value, setValue] = useState<
    | Loaded<
        typeof BubbleTeaOrder,
        { addOns: { $each: true }; instructions: true }
      >
    | undefined
  >();

  console.log("hook value id", value?.id);

  useEffect(() => {
    // deep clone order
    setValue(
      BubbleTeaOrder.create({
        ...order,
        instructions: CoPlainText.create(`${order.instructions}` || ""),
        addOns: ListOfBubbleTeaAddOns.create([...order.addOns]),
      }),
    );
  }, [order.id]);

  if (!value) return { value, save: () => {} };

  const save = () => {
    order.baseTea = value.baseTea;
    value.addOns.applyDiff(order.addOns);
    order.deliveryDate = value.deliveryDate;
    order.withMilk = value.withMilk;
    order.instructions = value.instructions;
  };

  return { value, save };
};

export function OrderFormWithSaveButton({
  order: originalOrder,
}: {
  order: Loaded<
    typeof BubbleTeaOrder,
    { addOns: { $each: true }; instructions: true }
  >;
}) {
  const { value: order, save } = useOrderForm(originalOrder);

  if (!order) return null;

  console.log("cloned order id", order.id);

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
      <h1>With save button</h1>

      <div>
        <div>original order id: {originalOrder.id}</div>
        <div>cloned order id: {order.id}</div>
      </div>

      <OrderThumbnail order={order} />

      <strong>base tea: {order.baseTea}</strong>

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
