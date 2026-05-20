"use client";

import { type CSSProperties, type ReactNode, useEffect, useState } from "react";

// Portable, framework-agnostic by the engine-island contract: no @/lib/cn, no
// lucide-react, no Tailwind utilities. Styling is inline CSS + the shared
// `diagram-pulse` class; icons are inline SVG. The consumer's `className` is
// forwarded untouched (sizing is the consumer's concern).
function cx(...parts: Array<string | false | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

// The bezel + notch are physical device chrome — fixed near-black in every
// host theme, never themed. The screen *inside* does follow the host theme;
// in dark mode it's a lighter dark than this bezel, so the frame still reads
// as a distinct bezel either way.
const BEZEL = "#0b0b0d";

// Signal bars + wifi waves, cropped from the original 80×18 icon set.
function SignalWifiIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="22"
      height="9"
      fill="currentColor"
      viewBox="0 0 46 18"
      aria-hidden="true"
    >
      <path
        fillRule="evenodd"
        d="M19.528 4.033c0-.633-.477-1.146-1.066-1.146h-1.067c-.59 0-1.067.513-1.067 1.146v9.934c0 .633.478 1.146 1.067 1.146h1.067c.589 0 1.066-.513 1.066-1.146zm-7.434 1.3h1.067c.589 0 1.066.525 1.066 1.173v7.434c0 .648-.477 1.173-1.066 1.173h-1.067c-.59 0-1.067-.525-1.067-1.173V6.506c0-.648.478-1.174 1.067-1.174M7.762 7.98H6.696c-.59 0-1.067.532-1.067 1.189v4.755c0 .656.477 1.188 1.067 1.188h1.066c.59 0 1.067-.532 1.067-1.188V9.17c0-.657-.478-1.189-1.067-1.189m-5.3 2.446H1.394c-.59 0-1.067.524-1.067 1.171v2.344c0 .647.478 1.171 1.067 1.171H2.46c.59 0 1.067-.524 1.067-1.171v-2.344c0-.647-.477-1.171-1.067-1.171M36.1 5.302c2.487 0 4.879.923 6.681 2.576a.355.355 0 0 0 .487-.004l1.297-1.263a.34.34 0 0 0-.003-.494c-4.73-4.375-12.195-4.375-16.926 0a.342.342 0 0 0-.003.494l1.298 1.263c.133.13.35.132.486.004 1.803-1.654 4.195-2.576 6.683-2.576m-.004 4.22c1.358 0 2.667.512 3.673 1.436.136.131.35.129.483-.006l1.287-1.32a.367.367 0 0 0-.005-.518 7.9 7.9 0 0 0-10.873 0 .367.367 0 0 0-.005.519l1.287 1.319a.343.343 0 0 0 .483.006 5.43 5.43 0 0 1 3.67-1.435m2.525 2.794a.4.4 0 0 1-.103.28l-2.176 2.456a.32.32 0 0 1-.242.112.32.32 0 0 1-.242-.112l-2.177-2.455a.4.4 0 0 1-.102-.28.4.4 0 0 1 .113-.277c1.39-1.314 3.426-1.314 4.816 0 .07.071.11.17.113.276"
        clipRule="evenodd"
      />
    </svg>
  );
}

// Battery casing + fill, cropped from the original icon set.
function BatteryIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="14"
      height="9"
      fill="currentColor"
      viewBox="54 0 26 18"
      aria-hidden="true"
    >
      <path
        d="M71.17 14.5v1h-12v-1zm5.5-5.5c0-1.039 0-1.767-.04-2.338-.04-.56-.113-.894-.223-1.152a3.3 3.3 0 0 0-1.747-1.747c-.258-.11-.591-.184-1.152-.223-.57-.04-1.299-.04-2.338-.04h-12c-1.04 0-1.767 0-2.338.04-.56.04-.894.113-1.152.223a3.3 3.3 0 0 0-1.747 1.747c-.11.258-.184.591-.223 1.152-.04.57-.04 1.299-.04 2.338s0 1.768.04 2.338c.04.561.113.894.223 1.152.334.787.96 1.413 1.747 1.748.258.11.591.183 1.152.222.57.04 1.299.04 2.338.04v1l-1.358-.005c-1.192-.016-1.92-.08-2.524-.338a4.3 4.3 0 0 1-2.19-2.085l-.085-.19c-.343-.806-.343-1.831-.343-3.882 0-1.922 0-2.943.282-3.727l.06-.155a4.3 4.3 0 0 1 2.087-2.19l.19-.085c.604-.257 1.331-.322 2.523-.338L59.17 2.5h12c2.05 0 3.076 0 3.882.343a4.3 4.3 0 0 1 2.275 2.275c.343.806.343 1.832.343 3.882s0 3.076-.343 3.882l-.086.19a4.3 4.3 0 0 1-2.19 2.085l-.154.061c-.784.282-1.805.282-3.727.282v-1c1.04 0 1.767 0 2.338-.04.56-.039.894-.113 1.152-.223a3.3 3.3 0 0 0 1.747-1.747c.11-.258.184-.591.223-1.152.04-.57.04-1.299.04-2.338"
        opacity=".35"
      />
      <path
        d="M78.67 7.281v4.076a2.21 2.21 0 0 0 1.328-2.038c0-.89-.523-1.693-1.328-2.038"
        opacity=".4"
      />
      <path d="M54.67 8.5c0-1.4 0-2.1.272-2.635a2.5 2.5 0 0 1 1.093-1.092C56.57 4.5 57.27 4.5 58.67 4.5h13c1.4 0 2.1 0 2.635.273a2.5 2.5 0 0 1 1.092 1.092c.273.535.273 1.235.273 2.635v1c0 1.4 0 2.1-.273 2.635a2.5 2.5 0 0 1-1.092 1.093c-.535.272-1.235.272-2.635.272h-13c-1.4 0-2.1 0-2.635-.272a2.5 2.5 0 0 1-1.093-1.093C54.67 11.6 54.67 10.9 54.67 9.5z" />
    </svg>
  );
}

// Aeroplane-mode glyph (inline so the island stays lucide-free).
function PlaneIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="11"
      height="11"
      fill="currentColor"
      viewBox="0 0 24 24"
      aria-hidden="true"
    >
      <path d="M21 16v-2l-8-5V3.5c0-.83-.67-1.5-1.5-1.5S10 2.67 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5z" />
    </svg>
  );
}

function StatusIcons({ offline }: { offline?: boolean }) {
  return (
    <span style={{ display: "flex", alignItems: "center", gap: "0.25rem", lineHeight: 1 }}>
      <span
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-end",
          width: "22px",
          height: "10px",
        }}
      >
        {offline ? <PlaneIcon /> : <SignalWifiIcon />}
      </span>
      <BatteryIcon />
    </span>
  );
}

function useWallClock(): string {
  // Render nothing on the server so the clock text can't mismatch the
  // client's locale-formatted output during hydration.
  const [time, setTime] = useState<Date | null>(null);
  useEffect(() => {
    setTime(new Date());
    let timeoutId: ReturnType<typeof setTimeout>;
    const tick = () => {
      setTime(new Date());
      timeoutId = setTimeout(tick, 60_000 - (Date.now() % 60_000));
    };
    timeoutId = setTimeout(tick, 60_000 - (Date.now() % 60_000));
    return () => clearTimeout(timeoutId);
  }, []);
  if (!time) return "";
  return time.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

const shellStyle: CSSProperties = {
  position: "relative",
  boxSizing: "border-box",
  display: "flex",
  flexDirection: "column",
  gap: "0.5rem",
  padding: "0.5rem",
  paddingTop: "1.25rem",
  borderWidth: "5px",
  borderStyle: "solid",
  borderColor: BEZEL,
  borderRadius: "1rem",
  background: "var(--diagram-card, #fff)",
  color: "var(--diagram-fg, #18181b)",
};

// Reusable phone chrome: a fixed near-black device bezel + notch (never
// themed — a phone's shell looks the same everywhere) wrapped around a screen
// that DOES follow the host theme. Status bar (clock + camera notch +
// signal/wifi/battery icons) along the top edge; when `offline` is set the
// signal/wifi icons collapse into a single plane icon — the convention iOS
// uses for aeroplane mode.
export function PhoneChrome({
  innerRef,
  pulseKey,
  className,
  offline,
  children,
}: {
  innerRef?: (el: HTMLDivElement | null) => void;
  pulseKey?: number;
  className?: string;
  offline?: boolean;
  children: ReactNode;
}) {
  const time = useWallClock();
  return (
    <div ref={innerRef} className={cx(className)} style={shellStyle}>
      {pulseKey !== undefined && pulseKey > 0 && (
        <span
          key={pulseKey}
          className="diagram-pulse"
          style={{
            position: "absolute",
            inset: "-3px",
            borderRadius: "1rem",
            pointerEvents: "none",
          }}
        />
      )}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          height: "1.25rem",
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          alignItems: "center",
          padding: "0 0.75rem",
        }}
      >
        <span
          style={{
            fontSize: "10px",
            fontWeight: 500,
            fontVariantNumeric: "tabular-nums",
            lineHeight: 1,
          }}
        >
          {time}
        </span>
        <span
          style={{
            alignSelf: "start",
            justifySelf: "center",
            height: "0.5rem",
            width: "3rem",
            borderBottomLeftRadius: "0.375rem",
            borderBottomRightRadius: "0.375rem",
            backgroundColor: BEZEL,
          }}
        />
        <span
          style={{
            justifySelf: "end",
            color: "var(--diagram-fg, #18181b)",
            opacity: 0.8,
          }}
        >
          <StatusIcons offline={offline} />
        </span>
      </div>
      {children}
    </div>
  );
}
