"use client";

import { Button } from "gcmp-design-system/src/app/components/atoms/Button";

export function FakeGetStartedButton() {
  return (
    <Button
      onClick={() => {
        alert(
          "During the public alpha, please use your email address as the API key, as shown in the docs!",
        );
        window.location.pathname = "/docs";
      }}
      variant="secondary"
    >
      Get Starter API Key
    </Button>
  );
}
