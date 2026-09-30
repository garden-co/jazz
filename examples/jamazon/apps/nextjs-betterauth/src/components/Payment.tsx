"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { RadioList, RadioListItem } from "@astryxdesign/core/RadioList";
import { Spinner } from "@astryxdesign/core/Spinner";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { Elements, PaymentElement, useElements, useStripe } from "@stripe/react-stripe-js";
// The pure entry loads Stripe.js only when a Stripe payment is shown, not on
// every page that imports this module (sandbox runs never contact Stripe).
import type { Stripe } from "@stripe/stripe-js";
import { loadStripe } from "@stripe/stripe-js/pure";
import { useState } from "react";
import type { Order, Payment as PaymentRow } from "@/schema";
import { requireBetterAuthToken } from "@/src/lib/auth-client";
import { formatMoney } from "@/src/store/pricing";

let stripePromise: Promise<Stripe | null> | undefined;
function stripe() {
  const key = process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY;
  if (!key) throw new Error("NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY is not set");
  return (stripePromise ??= loadStripe(key));
}

/**
 * Pay for a placed order. Whatever the provider, the browser never decides
 * that an order is paid: it asks the server to settle the payment, and the
 * server records the provider's answer on the order (idempotently).
 */
export function PaymentPanel({ order, payment }: { order: Order; payment?: PaymentRow }) {
  if (!payment)
    return (
      <Card padding={5}>
        <Spinner label="Preparing payment" />
      </Card>
    );
  return (
    <Card padding={5}>
      <VStack gap={4}>
        <Heading level={2}>Payment</Heading>
        {payment.status === "failed" && payment.failureReason && (
          <Banner
            status="error"
            title="Payment failed"
            description={`${payment.failureReason} You can try again.`}
          />
        )}
        {payment.provider === "stripe" && payment.clientSecret ? (
          <Elements stripe={stripe()} options={{ clientSecret: payment.clientSecret }}>
            <StripePayment order={order} />
          </Elements>
        ) : (
          <SandboxPayment order={order} />
        )}
      </VStack>
    </Card>
  );
}

async function settle(orderId: string, body: object): Promise<void> {
  const response = await fetch(`/api/orders/${orderId}/payment`, {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${await requireBetterAuthToken()}`,
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const { error } = (await response.json().catch(() => ({}))) as { error?: string };
    throw new Error(error ?? "The payment could not be recorded.");
  }
}

function SandboxPayment({ order }: { order: Order }) {
  const [outcome, setOutcome] = useState<"approve" | "decline">("approve");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  return (
    <VStack gap={4}>
      <Banner
        status="info"
        title="Sandbox payments"
        description="This store runs with the sandbox payment provider. No card is charged and no card details are collected: choose how the pretend card processor should answer."
      />
      <RadioList
        label="Sandbox card"
        value={outcome}
        onChange={(v) => setOutcome(v as typeof outcome)}
      >
        <RadioListItem value="approve" label="Approve" description="The payment succeeds." />
        <RadioListItem
          value="decline"
          label="Decline"
          description="The card is declined, so you can retry."
        />
      </RadioList>
      {error && <Banner status="error" title="Something went wrong" description={error} />}
      <Button
        label={`Pay ${formatMoney(order.totalCents)}`}
        size="lg"
        isLoading={busy}
        onClick={() => {
          setBusy(true);
          setError(undefined);
          settle(order.id, { sandboxOutcome: outcome })
            .catch((cause: Error) => setError(cause.message))
            .finally(() => setBusy(false));
        }}
      />
    </VStack>
  );
}

function StripePayment({ order }: { order: Order }) {
  const stripe = useStripe();
  const elements = useElements();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  async function pay() {
    if (!stripe || !elements) return;
    setBusy(true);
    setError(undefined);
    try {
      const result = await stripe.confirmPayment({ elements, redirect: "if_required" });
      if (result.error) setError(result.error.message);
      // Success or failure, the server reads the PaymentIntent back from
      // Stripe and records what really happened.
      await settle(order.id, {});
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }
  return (
    <VStack gap={4}>
      <Text type="supporting" color="secondary">
        Stripe test mode. Use card 4242 4242 4242 4242 with any future date and CVC.
      </Text>
      <PaymentElement />
      {error && <Banner status="error" title="Payment not completed" description={error} />}
      <Button
        label={`Pay ${formatMoney(order.totalCents)}`}
        size="lg"
        isLoading={busy}
        isDisabled={!stripe}
        onClick={() => void pay()}
      />
    </VStack>
  );
}
