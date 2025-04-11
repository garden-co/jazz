import { CoMap } from "jazz-tools";

interface CoMapVisualizerProps<T extends CoMap> {
  instance: T;
  showData?: boolean;
  showMetadata?: boolean;
}

export function CoMapVisualizer<T extends CoMap>({
  instance,
  showData = true,
  showMetadata = false,
}: CoMapVisualizerProps<T>) {
  return (
    <div className="flex justify-center">
      <div className="bg-black rounded p-2 min-w-[16px] min-h-[32px] shadow-md">
        {showMetadata && (
          <div className="text-white/50 text-xs mb-1">coId: {instance.id}</div>
        )}
        {showData && (
          <ul className="list-none p-0 m-0">
            {Object.entries(instance).map(([key, value]) => (
              <li
                key={key}
                className="text-white font-mono py-0.5 flex justify-between border-b border-white/10 last:border-b-0 text-sm"
              >
                <span className="font-bold mr-2">{key}:</span>
                <span>{String(value)}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
