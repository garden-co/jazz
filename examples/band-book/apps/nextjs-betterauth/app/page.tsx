import { redirect } from "next/navigation";

/** Signed-out visitors get the sign-in form from the Jazz provider on any page. */
export default function Home() {
  redirect("/workspace");
}
