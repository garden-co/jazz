import { CoList, CoMap } from "jazz-tools";
import { CoMapVisualizer } from "./CoMap";

interface CoListVisualizerProps<T extends CoList> {
  instance: T;
  showData?: boolean;
  showMetadata?: boolean;
  showChildMetadata?: boolean;
}

export function CoListVisualizer<T extends CoList>({
  instance,
  showData = true,
  showMetadata = false,
  showChildMetadata = false,
}: CoListVisualizerProps<T>) {
  return (
    <div className="flex justify-center p-4">
      <div className="bg-white rounded-lg p-6 min-w-[200px] shadow-md">
        {showMetadata && (
          <div className="text-stone-500 text-xs mb-4">coId: {instance.id}</div>
        )}
        <div className="flex flex-row gap-4 overflow-x-auto">
          {instance.map((coMap, index) => (
            <div key={index} className="flex-none">
              <div className="text-stone-500 text-xs mb-2">{index}</div>
              <CoMapVisualizer
                instance={coMap}
                showData={showData}
                showMetadata={showChildMetadata}
              />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
