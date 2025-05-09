import { Alert } from "@garden-co/design-system/src/components/atoms/Alert";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../../../contexts/Auth";

export default function Page() {
  const { auth, navigate } = useAuth();
  const searchParams = new URLSearchParams(window.location.search);
  const error = searchParams.get("error");
  if (!error) {
    auth.logIn().then(() => navigate("/"));
    return null;
  } else {
    return (
      <div className="min-h-screen flex flex-col justify-center">
        <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
          <Alert variant="warning" title="Sign In">
            {error}
          </Alert>
        </div>
      </div>
    );
  }
}
