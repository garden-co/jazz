import { OTPInput, REGEXP_ONLY_DIGITS } from "input-otp";
import { useState } from "react";

interface ConfirmationCodeInputProps {
  onSubmit: (code: string) => void;
}

export function ConfirmationCodeInput({
  onSubmit,
}: ConfirmationCodeInputProps) {
  const [value, setValue] = useState("");
  return (
    <OTPInput
      autoFocus
      value={value}
      onChange={setValue}
      onComplete={() => onSubmit(value)}
      pattern={REGEXP_ONLY_DIGITS}
      maxLength={6}
      containerClassName="group flex items-center has-[:disabled]:opacity-30"
      render={({ slots }) => (
        <div className="flex">
          {slots.map((slot, i) => (
            <div
              key={i}
              className={`${slot.isActive ? "outline-4 outline-blue-500" : "outline-0 outline-blue-500/20"} relative w-10 h-14 text-[2rem] flex items-center justify-center transition-all duration-100 border-zinc-200 border-y border-r first:border-l first:rounded-l-md last:rounded-r-md group-hover:border-blue-500/20 group-focus-within:border-blue-500/20 outline`}
            >
              {slot.char !== null ? <div>{slot.char}</div> : null}
            </div>
          ))}
        </div>
      )}
    />
  );
}
