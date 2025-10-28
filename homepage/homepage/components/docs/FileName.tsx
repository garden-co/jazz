import { FileCode } from "lucide-react";

export function FileName(props: { children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 font-bold">
      <FileCode className="h-4 w-4" /> {props.children}
    </div>
  );
}