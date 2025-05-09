import type { ComponentType } from "react";

export type Image = ComponentType<{
  src: any;
  alt: string;
  className?: string;
  width?: any;
  height?: any;
}>;

export const DefaultImage: Image = ({ src, alt, className, width, height }) => (
  <img
    className={className}
    src={src}
    alt={alt}
    width={width}
    height={height}
  />
);
