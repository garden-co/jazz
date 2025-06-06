import React from "react";
import { Input } from "../atoms/input";
import { Label } from "../atoms/label";

export function FormInput({
  label,
  id,
  type,
  required,
  value,
  onChange,
}: {
  label: string;
  id: string;
  type: string;
  required: boolean;
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
}) {
  return (
    <div className="grid gap-3">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type={type}
        required={required}
        value={value}
        onChange={onChange}
      />
    </div>
  );
}
