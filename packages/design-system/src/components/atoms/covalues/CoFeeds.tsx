import type { CoFeed, CoMap } from "jazz-tools";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { CoMapVisualizer } from "./CoMap";

interface CoFeedVisualizerProps<T extends CoFeed> {
  instance: T;
  showData?: boolean;
  showMetadata?: boolean;
  showChildMetadata?: boolean;
  showAllEntries?: boolean;
}

export function CoFeedVisualizer<T extends CoFeed>({
  instance,
  showData = true,
  showMetadata = false,
  showChildMetadata = false,
  showAllEntries = false,
}: CoFeedVisualizerProps<T>) {
  const currentAccountEntries = instance.byMe?.all || [];

  return (
    <div className="flex justify-center p-4">
      <div className="bg-white rounded-lg p-6 min-w-[200px] shadow-md">
        {showMetadata && (
          <div className="text-stone-500 text-xs mb-4">coId: {instance.id}</div>
        )}

        {showAllEntries ? (
          <div className="flex flex-col gap-4">
            <h3 className="text-sm font-medium">All Entries:</h3>
            <div className="flex flex-row gap-4 overflow-x-auto">
              {Array.from(currentAccountEntries).map((entry, index, array) => {
                const isLatest = index === array.length - 1;
                return (
                  <div
                    key={index}
                    className={`flex-none ${!isLatest ? "opacity-75" : ""}`}
                  >
                    <div className="text-stone-500 text-xs mb-2">{index}</div>
                    <CoMapVisualizer
                      instance={entry.value as CoMap}
                      showData={showData}
                      showMetadata={showChildMetadata}
                    />
                  </div>
                );
              })}
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <h3 className="text-sm font-medium">Latest Entry:</h3>
            {instance.byMe?.value && (
              <CoMapVisualizer
                instance={instance.byMe.value as CoMap}
                showData={showData}
                showMetadata={showChildMetadata}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}
