export const useParams = () => ({ framework: "react" });
export const usePathname = () => "/docs/react";
export const useRouter = () => ({
  push: () => {},
  replace: () => {},
  prefetch: () => {},
});
export const useSearchParams = () => new URLSearchParams();

