const Pagination = ({
  pages,
  page,
  setPage,
  windowSize = 2, // how many pages to show on either side
}: {
  pages: number;
  page: number;
  setPage: (p: number) => void;
  pageLength?: number;
  windowSize?: number;
}) => {
  if (pages <= 1) return null;

  const goToPage = (p: number) => {
    if (p >= 0 && p < pages) setPage(p);
  };

  const visiblePages = (): (number | string)[] => {
    const visible: (number | string)[] = [];

    const start = Math.max(0, page - windowSize);
    const end = Math.min(pages - 1, page + windowSize);

    // show first page and leading ellipsis
    if (start > 0) {
      visible.push(0);
      if (start > 1) visible.push("…");
    }

    // show window around current page
    for (let i = start; i <= end; i++) {
      visible.push(i);
    }

    // show trailing ellipsis and last page
    if (end < pages - 1) {
      if (end < pages - 2) visible.push("…");
      visible.push(pages - 1);
    }

    return visible;
  };

  return (
    <nav className="flex justify-center gap-2 mt-4">
      <button
        onClick={() => goToPage(page - 1)}
        disabled={page === 0}
        className={`px-3 py-1 rounded ${page === 0
          ? "bg-stone-100 text-stone-400 cursor-not-allowed"
          : "bg-stone-200 hover:bg-stone-300"
          }`}
      >
        Previous
      </button>

      {visiblePages().map((p, i) =>
        typeof p === "number" ? (
          <button
            key={i}
            onClick={() => goToPage(p)}
            className={`px-3 py-1 rounded ${p === page
              ? "bg-primary text-white"
              : "bg-stone-200 hover:bg-stone-300"
              }`}
          >
            {p + 1}
          </button>
        ) : (
          <span key={i} className="px-2 text-stone-500">
            {p}
          </span>
        )
      )}

      <button
        onClick={() => goToPage(page + 1)}
        disabled={page === pages - 1}
        className={`px-3 py-1 rounded ${page === pages - 1
          ? "bg-stone-100 text-stone-400 cursor-not-allowed"
          : "bg-stone-200 hover:bg-stone-300"
          }`}
      >
        Next
      </button>
    </nav>
  );
};

export default Pagination;
