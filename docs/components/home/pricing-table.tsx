"use client";

import { Fragment, type ReactNode } from "react";
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
  sharding: ReactNode;
  regions: ReactNode;
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
    features: "All product features",
    sharding: <Muted>No sharding</Muted>,
    regions: <Muted>Single region</Muted>,
    support: <Muted>Community support</Muted>,
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Jazz Pro",
    kind: "Source available",
    price: "Free",
    priceDetail:
      "Limited only by what you can fit on any number of shards up to 4 GB RAM in total.",
    features: "All product features",
    sharding: "Limited sharding (up to 4 GB RAM total)",
    regions: <Muted>Single region</Muted>,
    support: <Muted>Community support</Muted>,
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Jazz Scale",
    kind: "Self-serve license",
    price: "$200/mo",
    priceDetail: (
      <>
        1,000 <PasTerm>Peak Active Sessions</PasTerm> included
        <br />
        <strong>+$50/mo</strong> per additional 1,000 <PasTerm>PAS</PasTerm>
      </>
    ),
    features: "All product features",
    sharding: "Unlimited sharding",
    regions: "Multi-region",
    support: "Private support channel",
    operated: <Muted>Operated by you</Muted>,
  },
  {
    name: "Custom",
    kind: "Bespoke contract",
    price: "Let’s talk",
    priceDetail: "Volume deals and tailored terms for large companies and early-stage startups.",
    features: "All product features",
    sharding: "Unlimited sharding",
    regions: "Multi-region",
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

/** Brackets under pairs of tiers: who each pair is for. */
const brackets = [
  { after: 1, tiers: "Jazz and Jazz Pro", label: "typically enough for apps with 1K–10K MAUs" },
  { after: 3, tiers: "Jazz Scale and Custom", label: "scale to millions of users across regions" },
];

function TierColumn({ tier }: { tier: Tier }) {
  return (
    <div className="home-pricing-tier">
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
      <p className="home-pricing-cell">{tier.sharding}</p>
      <p className="home-pricing-cell">{tier.regions}</p>
      <p className="home-pricing-cell">{tier.support}</p>
      <p className="home-pricing-cell">{tier.operated}</p>
    </div>
  );
}

export function PricingTable() {
  return (
    <div className="home-pricing">
      {tiers.map((tier, i) => {
        const bracket = brackets.find((b) => b.after === i);
        return (
          <Fragment key={tier.name}>
            <TierColumn tier={tier} />
            {bracket ? (
              <p className="home-pricing-bracket">
                <span>
                  {/* Stacked on phones, the bracket can't show which tiers it
                    spans, so it names them. */}
                  <span className="home-pricing-bracket-tiers">{bracket.tiers}: </span>
                  {bracket.label}
                </span>
              </p>
            ) : null}
          </Fragment>
        );
      })}
    </div>
  );
}
