import { classNames } from "@/utils";

export function Table({ children }: React.PropsWithChildren<{}>) {
  return <table className={classNames("w-full")}>{children}</table>;
}

export function TableHead({ children }: React.PropsWithChildren<{}>) {
  return (
    <thead
      className={classNames(
        "text-left border-b border-gray-300 bg-stone-100 dark:bg-stone-925 dark:border-stone-800",
      )}
    >
      {children}
    </thead>
  );
}

export function TableBody({ children }: React.PropsWithChildren<{}>) {
  return (
    <tbody
      className={classNames("divide-y divide-gray-200 dark:divide-gray-900")}
    >
      {children}
    </tbody>
  );
}

export function TableRow({ children }: React.PropsWithChildren<{}>) {
  return <tr>{children}</tr>;
}

export function TableHeader({ children }: React.PropsWithChildren<{}>) {
  return (
    <th
      className={classNames(
        "font-medium text-stone-900 dark:text-white py-2 px-3",
      )}
    >
      {children}
    </th>
  );
}

export function TableCell({ children }: React.PropsWithChildren<{}>) {
  return <td className={classNames("py-2 px-3")}>{children}</td>;
}
