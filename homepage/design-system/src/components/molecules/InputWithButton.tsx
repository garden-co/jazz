import { ButtonProps } from "../atoms/Button";
import { Input, InputProps } from "./Input";

export function InputWithButton({
  inputProps,
  buttonProps,
}: {
  inputProps: InputProps;
  buttonProps: ButtonProps;
}) {
  return (
    <div className="flex gap-2 w-full">
      <Input {...inputProps} button={buttonProps} />
    </div>
  );
}
