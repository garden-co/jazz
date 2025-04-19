"use client";

import { track } from "@vercel/analytics";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";

export function FakeGetStartedButton({ tier }: { tier: "starter" | "indie" }) {
  return (
    <Button
      onClick={() => {
        track("FakeSignUp", { tier });
        alert(
          "During the public alpha, all limits are lifted. Please use your email address as the API key, as shown in the docs!",
        );
        window.location.pathname = "/docs";
      }}
      variant={tier === "starter" ? "secondary" : "primary"}
    >
      Get {tier === "starter" ? "Starter" : "Indie"} API Key
    </Button>
  );
}
