import { CoRichText } from "jazz-tools";

interface TextareaProps {
  coRichText?: CoRichText | null;
}

export default function Textarea({ coRichText }: TextareaProps) {
  if (!coRichText) return null;
  return (
    <textarea
      className="flex-1 border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900 whitespace-pre-wrap break-words resize-none"
      value={`${coRichText}`}
      onChange={(e) => coRichText.applyDiff(e.target.value)}
      rows={10}
    />
  );
}
