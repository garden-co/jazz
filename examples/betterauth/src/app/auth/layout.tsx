import Image from "next/image";
import Link from "next/link";

interface Props {
  children: React.ReactNode;
}

export default function AuthLayout({ children }: Props) {
  return (
    <div className="bg-muted flex min-h-svh flex-col items-center justify-center gap-6 p-6 md:p-10">
      <div className="flex w-full max-w-sm flex-col gap-6">
        <Link
          href="/"
          className="flex items-center gap-2 self-center font-medium"
        >
          <Image src="/jazz.svg" alt="Jazz Logo" width={24} height={24} />
          Jazz BetterAuth Demo
        </Link>
        {children}
      </div>
    </div>
  );
}
