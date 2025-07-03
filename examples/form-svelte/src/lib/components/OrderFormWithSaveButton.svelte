<script lang="ts">
  import { CoPlainText, type Loaded } from 'jazz-tools';
  import { 
    BubbleTeaAddOnTypes, 
    BubbleTeaBaseTeaTypes, 
    type BubbleTeaOrder 
  } from '$lib/schema';
  import { createForm } from '@tanstack/svelte-form';
  import OrderThumbnail from './OrderThumbnail.svelte';

  type LoadedBubbleTeaOrder = Loaded<
    typeof BubbleTeaOrder,
    { addOns: { $each: true }; instructions: true }
  >;

  type OrderFormData = {
    id: string;
    baseTea: (typeof BubbleTeaBaseTeaTypes)[number];
    addOns: (typeof BubbleTeaAddOnTypes)[number][];
    deliveryDate: Date;
    withMilk: boolean;
    instructions?: string;
  };

  type Props = {
    order: LoadedBubbleTeaOrder;
  };

  let { order: originalOrder }: Props = $props();

  const defaultValues: OrderFormData = originalOrder.toJSON();
  defaultValues.deliveryDate = new Date(defaultValues.deliveryDate);

  const form = createForm(() => ({
    defaultValues,
    onSubmit: async ({ value }) => {
      console.log('submit form', value);

      // Apply changes to the original Jazz order
      originalOrder.baseTea = value.baseTea;
      originalOrder.addOns.applyDiff(value.addOns);
      originalOrder.deliveryDate = value.deliveryDate;
      originalOrder.withMilk = value.withMilk;

      // Handle instructions (CoPlainText)
      const instructions = originalOrder.instructions ?? CoPlainText.create('');
      if (value.instructions) {
        instructions.applyDiff(value.instructions);
      }
    },
  }));

  function handleSubmit(event: Event) {
    event.preventDefault();
    event.stopPropagation();
    form.handleSubmit();
  }
</script>

<form onsubmit={handleSubmit} class="grid gap-5">
    <form.Subscribe selector={(state) => state.values}>
      {#snippet children(values)}
        <p>Unsaved order preview:</p>
        <OrderThumbnail order={values} />
      {/snippet}
    </form.Subscribe>

  <div class="flex flex-col gap-2">
    <label for="baseTea">Base tea</label>
    <form.Field 
      name="baseTea"
      validators={{
        onChange: ({ value }) => {
          if (!value) {
            return 'Please select your preferred base tea';
          }
          return undefined;
        },
      }}
    >
      {#snippet children(field)}
        <select
          id="baseTea"
          class="dark:bg-transparent"
          value={field.state.value}
          onchange={(e) => field.handleChange(e.currentTarget.value as typeof BubbleTeaBaseTeaTypes[number])}
          onblur={field.handleBlur}
        >
          <option value="" disabled>
            Please select your preferred base tea
          </option>
          {#each BubbleTeaBaseTeaTypes as teaType}
            <option value={teaType}>
              {teaType}
            </option>
          {/each}
        </select>
        {#if field.state.meta.errors.length > 0}
          <span class="text-red-500 text-sm">{field.state.meta.errors[0]}</span>
        {/if}
      {/snippet}
    </form.Field>
  </div>

  <fieldset>
    <legend class="mb-2">Add-ons</legend>
    <form.Field name="addOns">
      {#snippet children(field)}
        {#each BubbleTeaAddOnTypes as addOn}
          <div class="flex items-center gap-2">
            <input
              type="checkbox"
              id={addOn}
              checked={field.state.value.includes(addOn)}
              onchange={(e) => {
                const updatedAddons = (e.currentTarget.checked) ? [...field.state.value, addOn] : field.state.value.filter((item: string) => item !== addOn)
                field.handleChange(updatedAddons)}}
            />
            <label for={addOn}>{addOn}</label>
          </div>
        {/each}
      {/snippet}
    </form.Field>
  </fieldset>

  <div class="flex flex-col gap-2">
    <label for="deliveryDate">Delivery date</label>
    <form.Field 
      name="deliveryDate"
      validators={{
        onChange: ({ value }) => {
          if (!value) {
            return 'Delivery date is required';
          }
          return undefined;
        },
      }}
    >
      {#snippet children(field)}
        <input
          type="date"
          id="deliveryDate"
          class="dark:bg-transparent"
          value={field.state.value.toISOString().split('T')[0]}
          onchange={(e) => field.handleChange(new Date(e.currentTarget.value))}
          onblur={field.handleBlur}
        />
        {#if field.state.meta.errors.length > 0}
          <span class="text-red-500 text-sm">{field.state.meta.errors[0]}</span>
        {/if}
      {/snippet}
    </form.Field>
  </div>

  <div class="flex items-center gap-2">
    <form.Field name="withMilk">
      {#snippet children(field)}
        <input 
          type="checkbox" 
          id="withMilk" 
          checked={field.state.value}
          onchange={(e) => field.handleChange(e.currentTarget.checked)}
        />
        <label for="withMilk">With milk?</label>
      {/snippet}
    </form.Field>
  </div>

  <div class="flex flex-col gap-2">
    <label for="instructions">Special instructions</label>
    <form.Field name="instructions">
      {#snippet children(field)}
        <textarea
          id="instructions"
          class="dark:bg-transparent"
          value={field.state.value || ''}
          onchange={(e) => field.handleChange(e.currentTarget.value)}
        ></textarea>
      {/snippet}
    </form.Field>
  </div>

  <form.Subscribe selector={(state) => [state.canSubmit, state.isSubmitting]}>
    {#snippet children([canSubmit, isSubmitting])}
      <button
        type="submit"
        disabled={!canSubmit}
        class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded disabled:bg-gray-400"
      >
        {isSubmitting ? 'Submitting...' : 'Submit'}
      </button>
    {/snippet}
  </form.Subscribe>
</form>