export default function NotFound() {
  return (
    <article
      id="nd-page"
      data-full
      className="mx-auto flex w-full max-w-[1200px] [grid-area:main] px-4 py-6 md:px-6 md:pt-8 xl:px-8 xl:pt-14"
    >
      <div className="flex min-h-[50vh] flex-1 flex-col items-center justify-center gap-4 text-center">
        <h1 className="text-[1.75em] font-semibold">Resource not found</h1>
        <p className="max-w-[36rem] text-lg text-fd-muted-foreground">
          The resource you&apos;re looking for is not here anymore or has moved.
        </p>
        <p className="max-w-[36rem] text-sm leading-relaxed text-fd-muted-foreground">
          Browse the sidebar to find its new home, or jump back to a stable starting point.
        </p>
      </div>
    </article>
  );
}
