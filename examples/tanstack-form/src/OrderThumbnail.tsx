import { Loaded } from "jazz-tools";
import { OrderFormData } from "./OrderForm.tsx";
import { BubbleTeaOrder } from "./schema.ts";

export function OrderThumbnail({
  order,
}: {
  order: Loaded<typeof BubbleTeaOrder> | Partial<OrderFormData>;
}) {
  const { baseTea, addOns, instructions, deliveryDate, withMilk } = order;
  const date = deliveryDate?.toLocaleDateString("en-US", {
    timeZone: "UTC",
  });

  return (
    <a
      href={"id" in order ? `/#/order/${order.id}` : undefined}
      className="border p-3 flex justify-between items-start gap-3"
    >
      <div>
        <strong>
          {baseTea} {withMilk ? "milk " : ""} tea
        </strong>
        {addOns && addOns?.length > 0 && (
          <p className="text-sm text-stone-600">
            with {addOns?.join(", ").toLowerCase()}
          </p>
        )}
        {instructions && (
          <p className="text-sm text-stone-600 italic">{instructions}</p>
        )}
      </div>
      <div className="text-sm text-stone-600">{date}</div>
    </a>
  );
}
