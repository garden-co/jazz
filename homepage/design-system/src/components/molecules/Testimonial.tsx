import clsx from "clsx";
import Link from "next/link";

export function Testimonial({
  children,
  name,
  role,
  className,
  size = "md",
  imageUrl,
  darkImageUrl,
  url,
}: {
  children: React.ReactNode;
  name: string;
  role: string;
  className?: string;
  size?: "sm" | "md";
  imageUrl?: string;
  darkImageUrl?: string;
  url?: string;
}) {
  const content = (
    <>
      {imageUrl && (
        <img
          src={imageUrl}
          alt=""
          className={clsx(
            "rounded-full size-10 sm:size-12",
            darkImageUrl && "dark:hidden",
          )}
        />
      )}

      {darkImageUrl && (
        <img
          src={darkImageUrl}
          alt=""
          className={clsx("rounded-full size-10 sm:size-12 hidden dark:inline")}
        />
      )}
      <div className="text-sm sm:text-base">
        <div className="font-semibold text-highlight">{name}</div>
        <div className="text-stone-600 dark:text-stone-500">{role}</div>
      </div>
    </>
  );

  const authorClassName = clsx("inline-flex items-center gap-x-4", {
    "mt-6": size === "md",
    "mt-4": size === "sm",
  });

  return (
    <figure className={clsx("max-w-2xl", className)}>
      <svg
        className={clsx("text-primary", {
          "size-8": size === "md",
          "size-6": size === "sm",
        })}
        fill="currentColor"
        viewBox="0 0 32 32"
        aria-hidden="true"
      >
        <path d="M9.352 4C4.456 7.456 1 13.12 1 19.36c0 5.088 3.072 8.064 6.624 8.064 3.36 0 5.856-2.688 5.856-5.856 0-3.168-2.208-5.472-5.088-5.472-.576 0-1.344.096-1.536.192.48-3.264 3.552-7.104 6.624-9.024L9.352 4zm16.512 0c-4.8 3.456-8.256 9.12-8.256 15.36 0 5.088 3.072 8.064 6.624 8.064 3.264 0 5.856-2.688 5.856-5.856 0-3.168-2.304-5.472-5.184-5.472-.576 0-1.248.096-1.44.192.48-3.264 3.456-7.104 6.528-9.024L25.864 4z"></path>
      </svg>
      <blockquote
        className={clsx("text-balance text-highlight", {
          "mt-6 font-medium sm:text-lg leading-8 space-y-3": size === "md",
          "mt-4 leading-relaxed space-y-3": size === "sm",
        })}
      >
        {children}
      </blockquote>
      {url ? (
        <figcaption>
          <Link href={url} target="_blank" className={authorClassName}>
            {content}
          </Link>
        </figcaption>
      ) : (
        <figcaption className={authorClassName}>{content}</figcaption>
      )}
    </figure>
  );
}
