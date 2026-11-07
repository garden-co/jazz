"use client";

import type { ReactNode } from "react";
import { Button } from "@astryxdesign/core/Button";
import { Tooltip } from "@astryxdesign/core/Tooltip";

/** Text that is the same across tiers is muted; what sets a tier apart is not. */
const Muted = ({ children }: { children: ReactNode }) => (
  <span className="home-pricing-muted">{children}</span>
);

const PAS_DEFINITION =
  "A session is active in a minute when it writes, queries, subscribes or receives an update during that minute. Peak means the 95th percentile of those per-minute counts over the month.";

/** "Peak active sessions", underlined, with its full definition in a popover. */
function PasTerm({ children }: { children: ReactNode }) {
  return (
    <Tooltip content={PAS_DEFINITION} placement="below" touchTrigger="tap">
      <button type="button" className="home-pricing-term">
        {children}
      </button>
    </Tooltip>
  );
}

type Tier = {
  name: string;
  kind: string;
  price: string;
  priceDetail: ReactNode;
  features: ReactNode;
  support: ReactNode;
  operated: ReactNode;
  action?: ReactNode;
};

const tiers: Tier[] = [
  {
    name: "Jazz",
    kind: "Open-source (MIT)",
    price: "Free",
    priceDetail: "Limited only by what you can fit on a single server instance of any size.",
    features: <Muted>All product features. No sharding.</Muted>,
    support: <Muted>Community support</Muted>,
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Jazz Pro",
    kind: "Source available",
    price: "Free",
    priceDetail:
      "Limited only by what you can fit on any number of shards up to 4 GB RAM in total.",
    features: (
      <>
        <Muted>All product features.</Muted> Limited sharding.
      </>
    ),
    support: <Muted>Community support</Muted>,
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Jazz Scale",
    kind: "Self-serve license",
    price: "$200/mo",
    priceDetail: (
      <>
        1,000 <PasTerm>peak active sessions</PasTerm> included
        <br />
        <strong>+$50</strong> per additional 1,000 <PasTerm>PAS</PasTerm>
      </>
    ),
    features: (
      <>
        All product features. Unlimited sharding.
        <br />
        Global data distribution.
      </>
    ),
    support: "Private support channel",
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Custom",
    kind: "Bespoke contract",
    price: "Let’s talk",
    priceDetail: "Volume deals and tailored terms for large companies and early-stage startups.",
    features: (
      <>
        All product features. Unlimited sharding.
        <br />
        Global data distribution.
      </>
    ),
    support: (
      <>
        Private support channel
        <br />
        Custom support &amp; training
      </>
    ),
    operated: "Operated by you or us (with SLAs)",
    action: (
      <Button
        label="Schedule Call"
        variant="primary"
        size="sm"
        className="home-pricing-cta"
        href="https://cal.com/anselm-io/cloud-pro-intro"
      />
    ),
  },
];

export function PricingTable() {
  return (
    <div className="home-pricing" role="list">
      {tiers.map((tier) => (
        <div key={tier.name} className="home-pricing-tier" role="listitem">
          <div className="home-pricing-head">
            <p className="home-pricing-name">{tier.name}</p>
            <p>{tier.kind}</p>
          </div>
          <div className="home-pricing-cell">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="home-pricing-price">{tier.price}</p>
              {tier.action ? <div className="home-pricing-action">{tier.action}</div> : null}
            </div>
            <div>{tier.priceDetail}</div>
          </div>
          <p className="home-pricing-cell">{tier.features}</p>
          <p className="home-pricing-cell">{tier.support}</p>
          <p className="home-pricing-cell">{tier.operated}</p>
        </div>
      ))}
    </div>
  );
}
