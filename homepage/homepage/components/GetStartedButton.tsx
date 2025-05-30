import { Button } from "@garden-co/design-system/src/components/atoms/Button";

export function GetStartedButton({ tier }: { tier: "starter" | "indie" }) {
  return (
    <Button
      href={`https://dashboard.jazz.tools?utm_source=cloud_cta_${tier}`}
      newTab
      variant={tier === "starter" ? "secondary" : "primary"}
    >
      Get {tier === "starter" ? "Starter" : "Indie"} API key
    </Button>
  );
}
