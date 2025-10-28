"use client";

import { useState } from "react";
import { ErrorResponse } from "resend";
import { subscribe } from "../../actions/resend";
import { Icon } from "../atoms/Icon";
import { InputWithButton } from "../molecules/InputWithButton";

export function NewsletterForm() {
  const [email, setEmail] = useState("");
  const [error, setError] = useState<ErrorResponse | undefined>();

  const [state, setState] = useState<"ready" | "loading" | "success" | "error">(
    "ready",
  );

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();

    setState("loading");

    const res = await subscribe(email);

    if (res.error) {
      setError(res.error);
      setState("error");
    } else {
      setState("success");
    }
  };

  if (state === "success") {
    return (
      <div className="flex gap-3 items-center">
        <Icon name="check" className="text-green-500" />
        <p>Thanks for subscribing!</p>
      </div>
    );
  }

  if (state === "error" && error?.message) {
    return <p className="text-danger">Error: {error.message}</p>;
  }

  return (
    <form action="" onSubmit={submit} className="flex gap-x-4 w-120 max-w-md">
      <InputWithButton
        inputProps={{
          id: "email-address",
          name: "email",
          type: "email",
          value: email,
          onChange: (e) => setEmail(e.target.value),
          required: true,
          placeholder: "Enter your email",
          autoComplete: "email",
          className: "flex-1",
          label: "Email address",
          labelHidden: true,
          intent: "primary",
        }}
        buttonProps={{
          type: "submit",
          intent: "primary",
          variant: "outline",
          loadingText: "Subscribing...",
          loading: state === "loading",
          icon: "newsletter",
          iconPosition: "right",
          children: "Subscribe",
        }}
      />
    </form>
  );
}
