import { useAuth } from "jazz-react-auth-betterauth";
import { useRouter } from "next/navigation";

export default function Page() {
  const router = useRouter();
  const auth = useAuth();
  const searchParams = new URLSearchParams(window.location.search);
  const error = searchParams.get("error");
  if (!error) {
    auth.signIn().then(() => router.push("/"));
    return null;
  } else {
    return (
      <div className="min-h-screen flex flex-col justify-center">
        <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
          <div>{error}</div>
        </div>
      </div>
    );
  }
}
