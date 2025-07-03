import { CoPlainText, Loaded } from "jazz-tools";
import { useForm, SubmitHandler, Controller } from "react-hook-form";
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

// Would be great to derive this type from the CoValue schema
export type OrderFormData = {
  id: string;
  baseTea: (typeof BubbleTeaBaseTeaTypes)[number];
  addOns: (typeof BubbleTeaAddOnTypes)[number][];
  deliveryDate: Date;
  withMilk: boolean;
  instructions?: string;
};

export function OrderFormWithSaveButton({
  order: originalOrder,
}: {
  order: LoadedBubbleTeaOrder;
}) {
  const defaultValues = originalOrder.toJSON();
  // Convert timestamp to Date object
  defaultValues.deliveryDate = new Date(defaultValues.deliveryDate);
  const {
    register,
    handleSubmit,
    watch,
    control,
    formState: { errors },
  } = useForm<OrderFormData>({
    defaultValues,
  });

  const watchedValues = watch();

  const onSubmit: SubmitHandler<OrderFormData> = (data) => {
    console.log("submit form", data);

    // Apply changes to the original Jazz order
    originalOrder.baseTea = data.baseTea;
    originalOrder.addOns.applyDiff(data.addOns);
    originalOrder.deliveryDate = data.deliveryDate;
    originalOrder.withMilk = data.withMilk;

    // `applyDiff` requires nested objects to be CoValues as well
    const instructions = originalOrder.instructions ?? CoPlainText.create("");
    if (data.instructions) {
      instructions.applyDiff(data.instructions);
    }
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="grid gap-5">
      <div>
        <p>Unsaved order preview:</p>
        <OrderThumbnail order={watchedValues} />
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="baseTea">Base tea</label>
        <select
          {...register("baseTea", {
            required: "Please select your preferred base tea",
          })}
          id="baseTea"
          className="dark:bg-transparent"
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
        {errors.baseTea && (
          <span className="text-red-500 text-sm">{errors.baseTea.message}</span>
        )}
      </div>

      <fieldset>
        <legend className="mb-2">Add-ons</legend>
        {BubbleTeaAddOnTypes.map((addOn) => (
          <div key={addOn} className="flex items-center gap-2">
            <input
              type="checkbox"
              value={addOn}
              {...register("addOns")}
              id={addOn}
            />
            <label htmlFor={addOn}>{addOn}</label>
          </div>
        ))}
      </fieldset>

      <div className="flex flex-col gap-2">
        <label htmlFor="deliveryDate">Delivery date</label>
        <Controller
          name="deliveryDate"
          control={control}
          rules={{ required: "Delivery date is required" }}
          render={({ field }) => (
            <input
              type="date"
              id="deliveryDate"
              className="dark:bg-transparent"
              value={
                field.value instanceof Date
                  ? field.value.toISOString().split("T")[0]
                  : ""
              }
              onChange={(e) => field.onChange(new Date(e.target.value))}
            />
          )}
        />
        {errors.deliveryDate && (
          <span className="text-red-500 text-sm">
            {errors.deliveryDate.message}
          </span>
        )}
      </div>

      <div className="flex items-center gap-2">
        <input type="checkbox" {...register("withMilk")} id="withMilk" />
        <label htmlFor="withMilk">With milk?</label>
      </div>

      <div className="flex flex-col gap-2">
        <label htmlFor="instructions">Special instructions</label>
        <textarea
          {...register("instructions")}
          id="instructions"
          className="dark:bg-transparent"
        />
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
