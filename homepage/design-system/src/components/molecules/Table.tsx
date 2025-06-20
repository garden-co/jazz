"use client";
import { clsx } from "clsx";
import { toast } from "react-hot-toast";

export interface TableProps extends React.HTMLAttributes<HTMLTableElement> {
  className?: string;
  tableData: {
    headers: string[];
    data: {
      [key: string]: string | string[];
    }[];
  };
  copyable?: boolean;
}

export function Table({
  className,
  tableData,
  copyable,
  ...tableProps
}: TableProps) {
  return (
    <table
      className={clsx(
        "w-full border border-gray-200 rounded-lg overflow-hidden overflow-x-scroll",
        className,
      )}
      {...tableProps}
    >
      <thead>
        <tr>
          {tableData.headers.map((header) => (
            <th key={header} className="text-left pl-1 capitalize">
              {header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="border-t border-gray-200">
        {tableData.data.map((row, index) => (
          <tr
            key={`${row.id as string}-${index}=${tableData.headers.join("-")}`}
            className={clsx(
              index % 2 === 0
                ? "bg-stone-200/20 dark:bg-stone-800/40 hover:bg-stone-200/70 dark:hover:bg-stone-800/90"
                : "hover:bg-stone-200/50 dark:hover:bg-stone-100/20",
              "border-b border-stone-200 text-stone-800 hover:text-black dark:text-stone-200 dark:hover:text-white",
            )}
          >
            {tableData.headers.map((header, index) => (
              <td
                key={header}
                className={clsx(
                  index === 0 && "pl-1",
                  typeof row[header] !== "string" && "flex",
                )}
              >
                {typeof row[header] !== "string" ? (
                  <TableDataContainer>
                    {row[header]?.map((item) => (
                      <div
                        className={clsx(
                          "hover:underline",
                          copyable && "cursor-pointer",
                        )}
                        key={item}
                        onClick={() => {
                          if (copyable) {
                            navigator.clipboard.writeText(item.toString());
                            toast.success("Copied to clipboard");
                          }
                        }}
                      >
                        {item}
                      </div>
                    ))}
                  </TableDataContainer>
                ) : (
                  <TableDataContainer isCopyable={copyable}>
                    {row[header as keyof typeof row]}
                  </TableDataContainer>
                )}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const TableDataContainer = ({
  children,
  className,
  isCopyable,
}: { children: React.ReactNode; className?: string; isCopyable?: boolean }) => {
  return (
    <div
      className={clsx("flex gap-2", className, isCopyable && "cursor-pointer")}
      onClick={() => {
        if (isCopyable && children) {
          navigator.clipboard.writeText(children.toString());
          toast.success("Copied to clipboard");
        }
      }}
    >
      {children}
    </div>
  );
};
