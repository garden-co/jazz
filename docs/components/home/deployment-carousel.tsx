"use client";

import { Cloud, Server } from "lucide-react";
import { siCloudflare, siVercel } from "simple-icons";
import { type CSSProperties, useEffect, useRef, useState } from "react";
import {
  DEPLOYMENT_LAYOUT,
  DeploymentDiagram,
  deploymentScenarioNames,
  type DeploymentScenario,
} from "@/components/home/diagrams";

const scenarios: DeploymentScenario[] = ["vercel", "cloudflare", "aws", "server"];
const INTERVAL_MS = 4500;
const shortNames: Record<DeploymentScenario, string> = {
  vercel: "Vercel",
  cloudflare: "Cloudflare",
  aws: "AWS",
  server: "Single Server",
};

// Place the switcher over its space in the diagram, in percentages of the
// diagram, so it scales with it.
const { width, height, switcher } = DEPLOYMENT_LAYOUT;
const stageStyle: CSSProperties = { aspectRatio: `${width} / ${height}` };
const switcherStyle: CSSProperties = {
  left: `${(switcher.x / width) * 100}%`,
  top: `${(switcher.y / height) * 100}%`,
  width: `${(switcher.w / width) * 100}%`,
  height: `${(switcher.h / height) * 100}%`,
};

/**
 * Fig. 1: the same app deployed four ways, cross-fading from one to the next.
 * It starts once it scrolls into view, stops for good when the reader picks a
 * scenario, and never moves on its own for readers who prefer reduced motion.
 */
export function DeploymentCarousel() {
  const [active, setActive] = useState(0);
  const [visible, setVisible] = useState(false);
  const [paused, setPaused] = useState(false);
  const [picked, setPicked] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    const observer = new IntersectionObserver(([entry]) => setVisible(entry.isIntersecting), {
      threshold: 0.5,
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!visible || paused || picked) return;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const timer = window.setInterval(
      () => setActive((index) => (index + 1) % scenarios.length),
      INTERVAL_MS,
    );
    return () => window.clearInterval(timer);
  }, [visible, paused, picked]);

  return (
    <div
      ref={ref}
      className="home-carousel"
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
      onFocus={() => setPaused(true)}
      onBlur={() => setPaused(false)}
    >
      <div className="home-carousel-slides" style={stageStyle}>
        {scenarios.map((scenario, index) => (
          <div
            key={scenario}
            id={`deployment-${scenario}`}
            role="tabpanel"
            aria-hidden={index !== active}
            className="home-carousel-slide"
            data-active={index === active}
          >
            <DeploymentDiagram scenario={scenario} />
          </div>
        ))}
        {/* The switcher sits in the space the diagram leaves for it, under
            "Your own cloud:", and stays put while the diagrams cross-fade. */}
        <div
          className="home-carousel-tabs"
          role="tablist"
          aria-label="Where Jazz runs"
          style={switcherStyle}
        >
          {scenarios.map((scenario, index) => (
            <button
              key={scenario}
              type="button"
              role="tab"
              aria-selected={index === active}
              aria-controls={`deployment-${scenario}`}
              aria-label={deploymentScenarioNames[scenario]}
              className="home-carousel-tab"
              onClick={() => {
                setActive(index);
                setPicked(true);
              }}
            >
              <ScenarioIcon scenario={scenario} />
              {shortNames[scenario]}
              {scenario === "cloudflare" ? <span className="home-carousel-soon">soon</span> : null}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function ScenarioIcon({ scenario }: { scenario: DeploymentScenario }) {
  if (scenario === "server") {
    return <Server aria-hidden className="home-carousel-tab-icon" strokeWidth={1.75} />;
  }
  if (scenario === "aws") {
    return <Cloud aria-hidden className="home-carousel-tab-icon" strokeWidth={1.75} />;
  }
  const icon = scenario === "vercel" ? siVercel : siCloudflare;
  return (
    <svg aria-hidden className="home-carousel-tab-icon" viewBox="0 0 24 24" fill="currentColor">
      <path d={icon.path} />
    </svg>
  );
}
