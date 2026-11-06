"use client";

import { Cloud, Server } from "lucide-react";
import { siCloudflare, siVercel } from "simple-icons";
import {
  type CSSProperties,
  type ReactNode,
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import {
  DEPLOYMENT_LAYOUT,
  DeploymentDiagram,
  type DeploymentScenario,
} from "@/components/home/diagrams";

const scenarios: DeploymentScenario[] = ["vercel", "cloudflare", "aws", "server"];
const INTERVAL_MS = 4500;
const shortNames: Record<DeploymentScenario, string> = {
  vercel: "Vercel",
  cloudflare: "Cloudflare (soon)",
  aws: "AWS",
  server: "Single Server",
};

const stageStyle: CSSProperties = {
  aspectRatio: `${DEPLOYMENT_LAYOUT.width} / ${DEPLOYMENT_LAYOUT.height}`,
};

type CarouselState = {
  active: number;
  pick: (index: number) => void;
  setPaused: (paused: boolean) => void;
  slidesRef: React.RefObject<HTMLDivElement | null>;
};

const CarouselContext = createContext<CarouselState | null>(null);

function useCarousel() {
  const state = useContext(CarouselContext);
  if (!state) throw new Error("DeploymentSlides and DeploymentPicker need a DeploymentCarousel");
  return state;
}

/** Hovering or focusing the slides or the picker holds the current scenario. */
function pauseHandlers(setPaused: (paused: boolean) => void) {
  return {
    onMouseEnter: () => setPaused(true),
    onMouseLeave: () => setPaused(false),
    onFocus: () => setPaused(true),
    onBlur: () => setPaused(false),
  };
}

/**
 * Fig. 1: the same app deployed four ways, cross-fading from one to the next.
 * Holds the shared state for the slides and the picker, which may sit apart
 * on the page. It starts once the slides scroll into view, stops for good when
 * the reader picks a scenario, and never moves on its own for readers who
 * prefer reduced motion.
 */
export function DeploymentCarousel({ children }: { children: ReactNode }) {
  const [active, setActive] = useState(0);
  const [visible, setVisible] = useState(false);
  const [paused, setPaused] = useState(false);
  const [picked, setPicked] = useState(false);
  const slidesRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const element = slidesRef.current;
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

  const pick = (index: number) => {
    setActive(index);
    setPicked(true);
  };

  return (
    <CarouselContext.Provider value={{ active, pick, setPaused, slidesRef }}>
      {children}
    </CarouselContext.Provider>
  );
}

/** The four diagrams, sharing one grid cell so they cross-fade in place. */
export function DeploymentSlides() {
  const { active, setPaused, slidesRef } = useCarousel();
  return (
    <div
      ref={slidesRef}
      className="home-carousel-slides"
      style={stageStyle}
      {...pauseHandlers(setPaused)}
    >
      {scenarios.map((scenario, index) => (
        <div
          key={scenario}
          aria-hidden={index !== active}
          className="home-carousel-slide"
          data-active={index === active}
        >
          <DeploymentDiagram scenario={scenario} />
        </div>
      ))}
    </div>
  );
}

/** Picks the scenario the slides show. */
export function DeploymentPicker() {
  const { active, pick, setPaused } = useCarousel();
  return (
    <div className="home-carousel-picker" {...pauseHandlers(setPaused)}>
      <SegmentedControl
        label="Where Jazz runs"
        size="sm"
        value={scenarios[active]}
        onChange={(value) => pick(scenarios.indexOf(value as DeploymentScenario))}
      >
        {scenarios.map((scenario) => (
          <SegmentedControlItem
            key={scenario}
            value={scenario}
            label={shortNames[scenario]}
            icon={<ScenarioIcon scenario={scenario} />}
          />
        ))}
      </SegmentedControl>
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
