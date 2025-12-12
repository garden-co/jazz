"use client";
import { useEffect, useRef } from "react";

export const DemoToDo = () => {
  const HEIGHT = 350;
  let frameA = useRef<HTMLIFrameElement>(null);
  let frameB = useRef<HTMLIFrameElement>(null);
  useEffect(() => {
    const handler = (e: MessageEvent) => {
      if (e.data?.type === "id-generated" && frameA.current && frameB.current) {
        const id = e.data.id;
        if (e.source === frameA.current.contentWindow) {
          frameB.current.src = "/simplest-jazz-todo.html?id=" + id;
        }
      }
    };

    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);

  return (
    <div className="my-4 grid w-full grid-cols-1 gap-6 p-1 md:grid-cols-2">
      <div className="rounded-2xl outline-4">
        <iframe
          ref={frameA}
          id="childA"
          src="/simplest-jazz-todo.html"
          style={{ overflowY: "auto" }}
          height={HEIGHT}
          width="100%"
          title="Jazz Todo List Demo Instance A"
        ></iframe>
      </div>
      <div className="rounded-2xl outline-4">
        <iframe
          ref={frameB}
          id="childB"
          src="/simplest-jazz-todo.html"
          style={{ overflowY: "auto" }}
          height={HEIGHT}
          width="100%"
          title="Jazz Todo List Demo Instance B"
        ></iframe>
      </div>
    </div>
  );
};
