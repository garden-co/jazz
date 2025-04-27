import clsx from "clsx";
import { Heading } from "./Heading";

export function H1(
  props: React.ComponentPropsWithoutRef<"h1"> & React.PropsWithChildren,
) {
  return <Heading level={1} {...props} />;
}

export function H2(
  props: React.ComponentPropsWithoutRef<"h2"> & React.PropsWithChildren,
) {
  return <Heading level={2} {...props} />;
}

export function H3(
  props: React.ComponentPropsWithoutRef<"h3"> & React.PropsWithChildren,
) {
  return <Heading level={3} {...props} />;
}

export function H4(
  props: React.ComponentPropsWithoutRef<"h4"> & React.PropsWithChildren,
) {
  return <Heading level={4} {...props} />;
}

export function H5(
  props: React.ComponentPropsWithoutRef<"h5"> & React.PropsWithChildren,
) {
  return <Heading level={5} {...props} />;
}

export function H6(
  props: React.ComponentPropsWithoutRef<"h6"> & React.PropsWithChildren,
) {
  return <Heading level={6} {...props} />;
}
