import { clsx } from "clsx";
import MapTooltip from "./mapTooltip";
import { pingColorThresholds } from "./pingColorThresholds";
import { H2 } from "@garden-co/design-system/src/components/atoms/Headings";

export const LatencyMap = () => {
  return (
    <div className="container mb-12 overflow-hidden">
      <H2>Jazz Cloud</H2>
      <p>
        Real-time sync and storage infrastructure that scales up to millions of
        users.
      </p>

      <div className="relative mb-4 mt-8">
        <div className="aspect-12/4 relative xl:-mx-[10%] xl:w-[120%]">
          <MapTooltip />
        </div>
        <ul className="absolute bottom-0 left-0 m-0 flex list-none flex-col p-0 md:gap-1 lg:bottom-8">
          {pingColorThresholds.map((t, i) => (
            <li
              key={t.ping}
              className={clsx("flex items-center gap-1", {
                "hidden sm:flex": i % 2 !== 0,
              })}
            >
              <span
                className="size-2 rounded-full md:size-3"
                style={{
                  backgroundColor: `light-dark(${t.fill}, ${t.darkFill})`,
                }}
                aria-hidden="true"
              ></span>
              <span className="font-mono text-[9px] md:text-xs">
                &lt;{t.ping}ms
              </span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};
