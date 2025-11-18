export function SimpleCentered({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen w-screen flex-col justify-center gap-5 p-20">
      {children}
    </div>
  );
}
