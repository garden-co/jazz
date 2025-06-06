interface CardProps extends React.HTMLAttributes<HTMLDivElement> {}

export function Card({ children, ...props }: CardProps) {
  return (
    <div
      {...props}
      className={`bg-white rounded-lg border border-zinc-200 p-4 shadow ${props.className}`}
    >
      {children}
    </div>
  );
}
