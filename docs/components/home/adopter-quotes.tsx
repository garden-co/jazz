import { Text } from "@astryxdesign/core/Text";
import type { AdopterQuote } from "@/lib/home-quotes";

function initials(name: string) {
  return name
    .split(/\s+/)
    .map((part) => part[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();
}

function Portrait({ quote }: { quote: AdopterQuote }) {
  const kind = quote.imageKind ?? "avatar";
  return (
    <span className={`home-quote-portrait home-quote-portrait-${kind}`} aria-hidden>
      {quote.image ? <img src={quote.image} alt="" /> : initials(quote.name)}
    </span>
  );
}

function Attribution({ quote }: { quote: AdopterQuote }) {
  const who = (
    <>
      <Text as="span" display="block" weight="medium">
        {quote.name}
      </Text>
      <Text as="span" display="block" type="supporting" color="secondary">
        {quote.role}, {quote.company}
      </Text>
    </>
  );
  return (
    <figcaption className="home-quote-attribution">
      <Portrait quote={quote} />
      {quote.href ? (
        <a href={quote.href} className="home-quote-source">
          {who}
        </a>
      ) : (
        <span>{who}</span>
      )}
    </figcaption>
  );
}

/**
 * One featured quote, then the rest in a row. Renders nothing without
 * quotes, so the homepage never shows placeholder testimonials.
 */
export function AdopterQuotes({ quotes }: { quotes: AdopterQuote[] }) {
  const [featured, ...rest] = quotes;
  if (!featured) return null;
  return (
    <div className="home-quotes">
      <figure className="home-quote home-quote-featured">
        <blockquote>
          <p>&ldquo;{featured.quote}&rdquo;</p>
        </blockquote>
        <Attribution quote={featured} />
      </figure>
      {rest.length > 0 ? (
        <div className="home-quote-row">
          {rest.map((quote) => (
            <figure key={quote.quote} className="home-quote">
              <blockquote>
                <p>&ldquo;{quote.quote}&rdquo;</p>
              </blockquote>
              <Attribution quote={quote} />
            </figure>
          ))}
        </div>
      ) : null}
    </div>
  );
}
