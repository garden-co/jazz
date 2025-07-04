import { CoPlainText, Loaded, z } from "jazz-tools";
import { useForm } from "@tanstack/react-form";
import {
  BubbleTeaAddOnTypes,
  BubbleTeaBaseTeaTypes,
  BubbleTeaOrder,
} from "./schema.ts";
import { OrderThumbnail } from "./OrderThumbnail.tsx";

type LoadedBubbleTeaOrder = Loaded<
  typeof BubbleTeaOrder,
  { addOns: { $each: true }; instructions: true }
>;

const orderFormSchema = BubbleTeaOrder.getZodSchema()
  .extend({
    addOns: z
      .array(z.enum(BubbleTeaAddOnTypes))
      .min(1, "Please select at least one add-on"),
    deliveryDate: z.date("Delivery date is required"),
    // TanStack Form doesn't support CoPlainText fields, so we need to convert them to strings
    instructions: z.string().optional(),
  })
  .strict();

export type OrderFormData = z.infer<typeof orderFormSchema>;

export function OrderFormWithSaveButton({
  order: originalOrder,
}: {
  order: LoadedBubbleTeaOrder;
}) {
  const defaultValues: OrderFormData = originalOrder.toJSON();
  // Convert timestamp to Date
  defaultValues.deliveryDate = new Date(defaultValues.deliveryDate);

  const form = useForm({
    defaultValues,
    validators: {
      onChange: orderFormSchema,
    },
    onSubmit: async ({ value }) => {
      // Apply changes to the original Jazz order
      originalOrder.baseTea = value.baseTea;
      originalOrder.deliveryDate = value.deliveryDate;
      originalOrder.withMilk = value.withMilk;
      originalOrder.addOns.applyDiff(value.addOns);

      // `applyDiff` requires nested objects to be CoValues as well
      const instructions = originalOrder.instructions ?? CoPlainText.create("");
      if (value.instructions) {
        instructions.applyDiff(value.instructions);
      }
    },
  });

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        e.stopPropagation();
        form.handleSubmit();
      }}
      className="grid gap-5"
    >
      <div>
        <p>Unsaved order preview:</p>
        <form.Subscribe
          selector={(state) => [state.values]}
          children={([values]) => <OrderThumbnail order={values} />}
        />
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="baseTea">Base tea</label>
        <form.Field
          name="baseTea"
          children={(field) => (
            <>
              <select
                id="baseTea"
                className="dark:bg-transparent"
                value={field.state.value}
                onChange={(e) =>
                  field.handleChange(e.target.value as typeof field.state.value)
                }
                onBlur={field.handleBlur}
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
              {field.state.meta.errors.length > 0 && (
                <span className="text-red-500 text-sm">
                  {field.state.meta.errors[0]?.message}
                </span>
              )}
            </>
          )}
        />
      </div>

      <fieldset>
        <legend className="mb-2">Add-ons</legend>
        <form.Field
          name="addOns"
          mode="array"
          children={(field) => (
            <>
              {BubbleTeaAddOnTypes.map((addOn) => (
                <div key={addOn} className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id={addOn}
                    checked={field.state.value.includes(addOn)}
                    onChange={(e) => {
                      const currentValue = field.state.value;
                      const updatedValue = e.target.checked
                        ? [...currentValue, addOn]
                        : currentValue.filter((item) => item !== addOn);
                      field.handleChange(updatedValue);
                    }}
                  />
                  <label htmlFor={addOn}>{addOn}</label>
                </div>
              ))}
              {field.state.meta.errors.length > 0 && (
                <span className="text-red-500 text-sm">
                  {field.state.meta.errors[0]?.message}
                </span>
              )}
            </>
          )}
        />
      </fieldset>

      <div className="flex flex-col gap-2">
        <label htmlFor="deliveryDate">Delivery date</label>
        <form.Field
          name="deliveryDate"
          children={(field) => {
            // Check if the date is valid
            const dateString = !isNaN(field.state.value.getTime())
              ? field.state.value.toISOString().split("T")[0]
              : "";
            return (
              <>
                <input
                  type="date"
                  id="deliveryDate"
                  className="dark:bg-transparent"
                  value={dateString}
                  onChange={(e) => field.handleChange(new Date(e.target.value))}
                  onBlur={field.handleBlur}
                />
                {field.state.meta.errors.length > 0 && (
                  <span className="text-red-500 text-sm">
                    {field.state.meta.errors[0]?.message}
                  </span>
                )}
              </>
            );
          }}
        />
      </div>

      <div className="flex items-center gap-2">
        <form.Field
          name="withMilk"
          children={(field) => (
            <input
              type="checkbox"
              id="withMilk"
              checked={field.state.value}
              onChange={(e) => field.handleChange(e.target.checked)}
            />
          )}
        />
        <label htmlFor="withMilk">With milk?</label>
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="instructions">Special instructions</label>
        <form.Field
          name="instructions"
          children={(field) => (
            <textarea
              id="instructions"
              className="dark:bg-transparent"
              value={field.state.value || ""}
              onChange={(e) => field.handleChange(e.target.value)}
              onBlur={field.handleBlur}
            />
          )}
        />
      </div>

      <form.Subscribe
        selector={(state) => [state.canSubmit, state.isSubmitting]}
        children={([canSubmit, isSubmitting]) => (
          <button
            type="submit"
            disabled={!canSubmit}
            className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded disabled:bg-gray-400"
          >
            {isSubmitting ? "Submitting..." : "Submit"}
          </button>
        )}
      />
    </form>
  );
}
