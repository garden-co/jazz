"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useState } from "react";

type Product = {
  name: string;
  imageUrl: string;
  url: string;
  description: string;
};

function shuffle<T>(array: T[]): T[] {
  const shuffled = [...array];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

export function ShowcaseGrid({ products }: { products: Product[] }) {
  const [items, setItems] = useState(products);

  useEffect(() => {
    setItems(shuffle(products));
  }, [products]);

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {items.map((product) => (
        <Link
          key={product.url}
          href={product.url}
          target="_blank"
          rel="noopener noreferrer"
          className="group flex flex-col overflow-hidden rounded-lg border border-stone-200 bg-stone-50 transition-colors hover:border-stone-300 hover:bg-stone-100 dark:border-stone-800 dark:bg-stone-900 dark:hover:border-stone-700 dark:hover:bg-stone-850"
        >
          <div className="aspect-[4/3] overflow-hidden">
            <Image
              className="h-full w-full object-cover transition-transform duration-300 group-hover:scale-[1.03]"
              src={product.imageUrl}
              width="600"
              height="450"
              alt={product.name}
            />
          </div>
          <div className="flex flex-1 flex-col gap-1 p-3">
            <div className="flex items-baseline justify-between gap-2">
              <h3 className="font-medium text-stone-900 dark:text-stone-100">
                {product.name}
              </h3>
              <span className="shrink-0 text-xs text-stone-400 dark:text-stone-500">
                {product.url.replace("https://", "").replace(/\/$/, "")}
              </span>
            </div>
            <p className="text-sm text-stone-500 dark:text-stone-400">
              {product.description}
            </p>
          </div>
        </Link>
      ))}
    </div>
  );
}
