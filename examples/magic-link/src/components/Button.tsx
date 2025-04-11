interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  color?: "default" | "primary" | "destructive";
}

const colors = {
  default: "bg-zinc-100 hover:bg-zinc-50 text-black border-zinc-200",
  primary: "bg-blue-600 hover:bg-blue-500 text-white border-blue-600",
  destructive: "bg-red-600 hover:bg-red-500 text-white border-red-600",
};

export function Button({ color = "default", ...props }: ButtonProps) {
  return (
    <button
      {...props}
      className={`${colors[color]} transition-colors duration-75 ease-out border rounded-md py-1.5 px-3 text-sm`}
    />
  );
}
