/** Shared form component class patterns */

/** Base interactive styles */
const baseInteractive = "outline-none transition-[color,box-shadow]";

/** Focus ring */
export const focusRing =
  "focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50";

/** Disabled state */
export const disabledStyles = "disabled:cursor-not-allowed disabled:opacity-50";

/** Base input field styles */
export const formFieldBase =
  "w-full min-w-0 rounded-md border border-input bg-transparent px-3 py-2 text-base text-stone-960 shadow-sm placeholder:text-muted-foreground dark:bg-input/30 dark:text-white md:text-sm";

/** Invalid/error state */
export const formFieldInvalid =
  "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40";

/** Complete form field (Input, Textarea, Select) */
export const formField = `${formFieldBase} ${baseInteractive} ${focusRing} ${disabledStyles} ${formFieldInvalid}`;

/** Checkbox and radio base */
export const checkableBase = `shadow-sm ${baseInteractive} ${disabledStyles}`;

/** Interactive focus (checkboxes, radios, switches) */
export const interactiveFocus = `${baseInteractive} ${focusRing}`;
