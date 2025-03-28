"use client";

import { cn } from "@/lib/utils";
import { useMemo } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./tooltip";

const ranges = [
  {
    from: 0,
    className: "bg-green-400",
  },
  {
    from: 50,
    className: "bg-green-500",
  },
  {
    from: 100,
    className: "bg-green-600",
  },
  {
    from: 250,
    className: "bg-green-700",
  },
  {
    from: 500,
    className: "bg-green-800",
  },
  {
    from: 750,
    className: "bg-green-900",
  },
  {
    from: 1500,
    className: "bg-green-950",
  },
  {
    from: 3000,
    className: "bg-[#ff001e]",
  },
];
interface Props {
  data: [number[], number[]];
}
export default function LatencyChart({ data }: Props) {
  const series = useMemo(() => {
    return data[1].map((value, index) => ({
      ts: data[0][index],
      value: Math.round(value),
    }));
  }, [data]);

  return (
    <>
      <TooltipProvider delayDuration={0} skipDelayDuration={0}>
        <figure className="flex items-stretch w-full gap-px justify-end">
          <Tooltip>
            <TooltipTrigger asChild>
              <div
                className={cn(
                  "rounded-md grow hover:opacity-50 dark:bg-gray-900 bg-gray-200",
                )}
              />
            </TooltipTrigger>
            <TooltipContent>
              <p>
                <span className="font-semibold">No data</span>
              </p>
            </TooltipContent>
          </Tooltip>
          {series.map(({ value, ts }) => {
            const valueClass = getClassForValue(value);
            return (
              <Tooltip key={ts}>
                <TooltipTrigger asChild>
                  <div
                    className={cn(
                      "h-6 w-1 lg:w-2 rounded-md hover:opacity-50",
                      valueClass,
                    )}
                  />
                </TooltipTrigger>
                <TooltipContent>
                  <time
                    className="text-xs"
                    dateTime={new Date(ts).toISOString()}
                  >
                    {new Date(ts).toLocaleString()}
                  </time>
                  <p className="text-sm text-right">
                    <span
                      className={cn(
                        "rounded-md size-3 inline-block mr-1",
                        valueClass,
                      )}
                    />
                    <span className="font-semibold">{value}</span> ms
                  </p>
                </TooltipContent>
              </Tooltip>
            );
          })}
        </figure>
      </TooltipProvider>
    </>
  );
}

function getClassForValue(value: number) {
  for (let i = 0; i < ranges.length; i++) {
    const { from } = ranges[i];
    if (value < from) {
      return ranges[i - 1].className;
    }
  }

  return ranges[ranges.length - 1].className;
}
