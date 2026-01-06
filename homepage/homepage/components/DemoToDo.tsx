"use client";
import { useEffect, useRef, useState } from "react";

export const DemoToDo = () => {
  const HEIGHT = 350;
  const frameA = useRef<HTMLIFrameElement>(null);
  const frameB = useRef<HTMLIFrameElement>(null);

  const [activeListId, setActiveListId] = useState<string | null>(null);

  useEffect(() => {
    const handler = (e: MessageEvent) => {
      if (e.origin !== window.location.origin) return;
      if (e.data?.type !== "id-generated") return;
      setActiveListId(e.data.id);
      return;
    };

    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);

  return (
    <div className="my-4 grid w-full grid-cols-1 gap-6 p-1 md:grid-cols-2">
      <div className="overflow-hidden rounded-2xl outline-4">
        <iframe
          ref={frameA}
          src="/minimal-example/index.html"
          height={HEIGHT}
          width="100%"
          title="Jazz Todo List Demo Instance A"
        />
      </div>

      <div className="overflow-hidden rounded-2xl outline-4">
        {activeListId ? (
          <iframe
            ref={frameB}
            src={`/minimal-example/index.html?id=${activeListId}`}
            height={HEIGHT}
            width="100%"
            title="Jazz Todo List Demo Instance B"
          />
        ) : (
          <div
            style={{ height: HEIGHT }}
            className="flex items-center justify-center bg-gray-50 text-gray-400"
          ></div>
        )}
      </div>
    </div>
  );
};
