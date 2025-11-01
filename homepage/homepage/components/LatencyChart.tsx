"use client";

import { cn } from "@/lib/utils";
import { useMemo } from "react";
import * as HoverCard from "@radix-ui/react-hover-card";

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
  intervalMin: number;
  latencyOverTime: [number[], number[]];
  upOverTime: [number[], number[]];
  upCountOverTime: [number[], number[]];
  isUp?: boolean;
}
export default function LatencyChart({ latencyOverTime, upOverTime, upCountOverTime, intervalMin, isUp = true }: Props) {
  const series = useMemo(() => {
    return latencyOverTime[1].map((value, index) => ({
      ts: latencyOverTime[0][index],
      value: Math.round(value),
      up: upOverTime[1][index],
      upCount: upCountOverTime[1][index],
    }));
  }, [latencyOverTime, upOverTime]);

  return (
    <figure className="flex items-stretch w-full justify-end">
      <HoverCard.Root openDelay={0} closeDelay={0}>
        <HoverCard.Trigger asChild>
          <div
            className={cn(
              "rounded-md grow hover:opacity-50 dark:bg-gray-900 bg-gray-200",
            )}
          />
        </HoverCard.Trigger>
        <HoverCard.Content className="border border-stone-500 bg-white dark:bg-black shadow-lg absolute w-[150px] -ml-[75px] l-[50%] rounded-md p-2">
          <HoverCard.Arrow className="fill-stone-500" />
          <p className="text-sm text-center">
            No data
          </p>
        </HoverCard.Content>
      </HoverCard.Root>
      {series.map(({ value, ts, up, upCount }, index) => {
        const isLast = index === series.length - 1;
        const upPercentage = up / upCount;
        
        // If this is the last bar and we're down, override the colour to red
        const valueClass = getClassForLatencyAndUp(value, isLast && !isUp ? 0 : upPercentage);

        const downtimeMin = (1 - upPercentage) * intervalMin;

        const from = new Date(ts);
        const to = new Date(ts + intervalMin * 60 * 1000);

        let fromH = Intl.DateTimeFormat("en-US", {
          hour: "numeric",
          timeZone: "UTC",
        }).formatToParts(from);
        const toH = Intl.DateTimeFormat("en-US", {
          hour: "numeric",
          timeZone: "UTC",
        }).formatToParts(to);


        // remove the (PM) or (AM) if the "from" if its the same as the "to"
        if(fromH[2].value === toH[2].value) {
          fromH = fromH.filter(({type}) => type !== "literal").slice(0, 1);
        }
        
        return (
          <HoverCard.Root key={ts} openDelay={0} closeDelay={0}>
            <HoverCard.Trigger asChild >
              <div className="p-[0.5px]">
                <div
                  className={cn(
                    "h-4 w-1 lg:w-2 rounded-xs hover:opacity-50",
                    valueClass,
                  )}
                />
              </div>
            </HoverCard.Trigger>
            <HoverCard.Content className="border border-stone-500 bg-white dark:bg-black shadow-lg absolute w-[180px] -ml-[90px] l-[50%] rounded-md py-1 px-2">
              <HoverCard.Arrow className="fill-stone-500" />
                <div className="text-right">
                  <time
                    className="text-xs flex justify-between"
                    dateTime={from.toISOString()}
                    >
                      <span>
                        {Intl.DateTimeFormat("en-US", {
                          dateStyle: "medium",
                          timeZone: "UTC",
                        }).format(from)}
                      </span>

                      <span className="whitespace-nowrap">
                        {fromH.map((part, index) => (
                          <span key={index}>{part.value}</span>
                        ))}
                        {' – '}
                        {toH.map((part, index) => (
                          <span key={index}>{part.value}</span>
                        ))}
                      </span>
                  </time>
                </div>
                <div className="text-sm text-right flex items-center justify-between">
                  <span className="flex items-center">
                    <span
                      className={cn(
                        "rounded-md size-2.5 inline-block mr-1",
                        getClassForLatencyAndUp(value, 1),
                      )}
                    />
                    Latency
                  </span>
                  <span>
                    <span className="font-semibold">{value}</span> ms
                    
                  </span>
                </div>
                {upPercentage < 0.99 && (
                  <div className="text-sm text-right flex items-center justify-between">
                    <span className="flex items-center">
                      <span
                        className={cn(
                          "rounded-md size-2.5 inline-block mr-1",
                          getClassForLatencyAndUp(0, upPercentage),
                        )}
                      />
                      Downtime
                      </span>
                    <span>
                    <span className="font-semibold">
                      {Math.round(downtimeMin)}</span> min
                    </span>
                </div>
                  
                )}
                
                <p className="text-sm text-right">
                  
                </p>                
              </HoverCard.Content>
          </HoverCard.Root>
        );
      })}
    </figure>
  );
}

function getClassForLatencyAndUp(value: number, up: number) {
  if (up >= 0.99) {
    for (let i = 0; i < ranges.length; i++) {
      const { from } = ranges[i];
      if (value < from) {
        return ranges[i - 1].className;
      }
    }
  } else if (up >= 0.9) {
    return "bg-yellow-400";
  } else if (up >= 0.8) {
    return "bg-orange-500";
  }

  return "bg-[#ff001e]";
}
