<script lang="ts">
  import { CoPlainText, type Loaded } from 'jazz-tools';
  import { 
    BubbleTeaAddOnTypes, 
    BubbleTeaBaseTeaTypes, 
    type BubbleTeaOrder 
  } from '$lib/schema';
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

  // Initialize form data from the original order
  const defaultValues = originalOrder.toJSON();
  defaultValues.deliveryDate = new Date(defaultValues.deliveryDate);

  // Reactive form state
  let formData: OrderFormData = $state({
    id: defaultValues.id,
    baseTea: defaultValues.baseTea,
    addOns: [...defaultValues.addOns],
    deliveryDate: defaultValues.deliveryDate,
    withMilk: defaultValues.withMilk,
    instructions: defaultValues.instructions || '',
  });

  // Form validation state
  let errors = $state({
    baseTea: '',
    deliveryDate: '',
  });

  let isSubmitting = $state(false);

  // Derived state for form validity
  const isFormValid = $derived(
    formData.baseTea && 
    formData.deliveryDate && 
    !errors.baseTea && 
    !errors.deliveryDate
  );

  // Validation functions
  function validateBaseTea(value: string) {
    if (!value) {
      errors.baseTea = 'Please select your preferred base tea';
    } else {
      errors.baseTea = '';
    }
  }

  function validateDeliveryDate(value: Date) {
    if (!value) {
      errors.deliveryDate = 'Delivery date is required';
    } else {
      errors.deliveryDate = '';
    }
  }

  // Event handlers
  function handleBaseTeaChange(event: Event) {
    const target = event.target as HTMLSelectElement;
    formData.baseTea = target.value as typeof formData.baseTea;
    validateBaseTea(formData.baseTea);
  }

  function handleAddOnChange(addOn: string, checked: boolean) {
    if (checked) {
      formData.addOns = [...formData.addOns, addOn as typeof formData.addOns[0]];
    } else {
      formData.addOns = formData.addOns.filter(item => item !== addOn);
    }
  }

  function handleDeliveryDateChange(event: Event) {
    const target = event.target as HTMLInputElement;
    formData.deliveryDate = new Date(target.value);
    validateDeliveryDate(formData.deliveryDate);
  }

  function handleWithMilkChange(event: Event) {
    const target = event.target as HTMLInputElement;
    formData.withMilk = target.checked;
  }

  function handleInstructionsChange(event: Event) {
    const target = event.target as HTMLTextAreaElement;
    formData.instructions = target.value;
  }

  // Form submission
  async function handleSubmit(event: Event) {
    event.preventDefault();
    
    // Validate all fields
    validateBaseTea(formData.baseTea);
    validateDeliveryDate(formData.deliveryDate);

    if (!isFormValid) {
      return;
    }

    isSubmitting = true;

    try {
      console.log('submit form', formData);

      // Apply changes to the original Jazz order
      originalOrder.baseTea = formData.baseTea;
      originalOrder.addOns.applyDiff(formData.addOns);
      originalOrder.deliveryDate = formData.deliveryDate;
      originalOrder.withMilk = formData.withMilk;

      // Handle instructions (CoPlainText)
      const instructions = originalOrder.instructions ?? CoPlainText.create('');
      if (formData.instructions) {
        instructions.applyDiff(formData.instructions);
      }
    } catch (error) {
      console.error('Error submitting form:', error);
    } finally {
      isSubmitting = false;
    }
  }
</script>

<form onsubmit={handleSubmit} class="grid gap-5">
  <div>
    <p>Unsaved order preview:</p>
    <OrderThumbnail order={formData} />
  </div>

  <div class="flex flex-col gap-2">
    <label for="baseTea">Base tea</label>
    <select
      id="baseTea"
      class="dark:bg-transparent"
      value={formData.baseTea}
      onchange={handleBaseTeaChange}
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
    {#if errors.baseTea}
      <span class="text-red-500 text-sm">{errors.baseTea}</span>
    {/if}
  </div>

  <fieldset>
    <legend class="mb-2">Add-ons</legend>
    {#each BubbleTeaAddOnTypes as addOn}
      <div class="flex items-center gap-2">
        <input
          type="checkbox"
          id={addOn}
          checked={formData.addOns.includes(addOn)}
          onchange={(e) => handleAddOnChange(addOn, e.currentTarget.checked)}
        />
        <label for={addOn}>{addOn}</label>
      </div>
    {/each}
  </fieldset>

  <div class="flex flex-col gap-2">
    <label for="deliveryDate">Delivery date</label>
    <input
      type="date"
      id="deliveryDate"
      class="dark:bg-transparent"
      value={formData.deliveryDate.toISOString().split('T')[0]}
      onchange={handleDeliveryDateChange}
    />
    {#if errors.deliveryDate}
      <span class="text-red-500 text-sm">{errors.deliveryDate}</span>
    {/if}
  </div>

  <div class="flex items-center gap-2">
    <input 
      type="checkbox" 
      id="withMilk" 
      checked={formData.withMilk}
      onchange={handleWithMilkChange}
    />
    <label for="withMilk">With milk?</label>
  </div>

  <div class="flex flex-col gap-2">
    <label for="instructions">Special instructions</label>
    <textarea
      id="instructions"
      class="dark:bg-transparent"
      value={formData.instructions || ''}
      onchange={handleInstructionsChange}
    ></textarea>
  </div>

  <button
    type="submit"
    disabled={!isFormValid}
    class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded disabled:bg-gray-400"
  >
    {isSubmitting ? 'Submitting...' : 'Submit'}
  </button>
</form> 
