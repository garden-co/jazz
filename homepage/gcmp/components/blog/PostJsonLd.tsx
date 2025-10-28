export function PostJsonLd({
  title,
  subtitle,
  image,
  author,
  datePublished,
  description,
}: {
  title: string;
  subtitle: string;
  image: string;
  author: string;
  datePublished: string;
  description?: string;
}) {
  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "Article",
    headline: title + " " + subtitle,
    image,
    author: {
      "@type": "Person",
      name: author,
    },
    publisher: {
      "@type": "Organization",
      name: "Garden Computing",
    },
    datePublished,
    description: description || subtitle,
  };

  return (
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
    />
  );
}
