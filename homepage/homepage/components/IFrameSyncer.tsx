'use client';
import { useEffect } from 'react';

export const IFrameSyncer = () => {
  useEffect(() => {
    const frameA = document.getElementById("childA") as HTMLIFrameElement;
    const frameB = document.getElementById("childB") as HTMLIFrameElement;

    const handler = (e: MessageEvent) => {
      if (e.data?.type === "id-generated" && frameA && frameB) {
        const id = e.data.id;
        if (e.source === frameA.contentWindow) {
          frameB.src = "/simplest-jazz-todo.html?id=" + id;
        }
      }
    };

    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);

  return null; // Renders nothing visibly
};