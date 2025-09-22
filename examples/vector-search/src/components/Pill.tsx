export function Pill({
  children,
  icon,
}: {
  icon?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="flex gap-2 items-center max-w-max bg-zinc-50 rounded-full pl-3 pr-4 py-1.5">
      {icon}

      {children}
    </div>
  );
}

export function PillColumn({ children }: { children: React.ReactNode }) {
  return <div className="flex flex-col">{children}</div>;
}

export function PillLabel({ children }: { children: React.ReactNode }) {
  return <span className="uppercase text-xs text-zinc-500">{children}</span>;
}

export function PillValue({ children }: { children: React.ReactNode }) {
  return <div className="text-sm">{children}</div>;
}
