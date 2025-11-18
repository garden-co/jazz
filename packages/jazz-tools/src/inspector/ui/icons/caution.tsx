export function CautionIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeLinejoin="round"
    >
      <circle cx="12" cy="12" r="12" fill="currentColor" />
      <rect x="10.5" y="6" width="3" height="7.5" rx="1.5" fill="#fff" />
      <rect x="10.5" y="16.5" width="3" height="3" rx="1.5" fill="#fff" />
    </svg>
  );
}
