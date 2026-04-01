export function CollaboratorBadge({ name, color }: { name: string; color: string }) {
  return (
    <span className="flex items-center gap-2">
      <span
        className="inline-block h-3 w-3 rounded-full border border-stone-200"
        style={{ backgroundColor: color }}
      />
      <span>{name}</span>
    </span>
  );
}
