<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Account } from "jazz-tools";
  import { goto } from "$app/navigation";

  import { toast } from "svelte-sonner";
  import { betterAuthClient } from "$lib/auth-client";
  import { AccountCoState } from "jazz-tools/svelte";

  const me = new AccountCoState(Account, { resolve: { profile: true } });

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    const target = e.target as HTMLFormElement;
    const formData = new FormData(target);
    const email = formData.get("email") as string;
    const password = formData.get("password") as string;
    const name = formData.get("name") as string;
    const confirmPassword = formData.get("confirmPassword") as string;

    if (password !== confirmPassword) {
      toast.error("Error", {
        description: "Passwords do not match",
      });
      return;
    }

    await betterAuthClient.signUp.email(
      {
        email,
        password,
        name,
      },
      {
        onSuccess: async () => {
          if (me.current.$isLoaded) {
            me.current.profile.$jazz.set("name", name);
          }
          goto("/");
        },
        onError: (error) => {
          toast.error("Sign up error", {
            description: error.error.message,
          });
        },
      },
    );
  };
</script>

<div class="flex flex-col gap-6">
  <Card>
    <CardHeader class="text-center">
      <CardTitle class="text-xl">Welcome</CardTitle>
      <CardDescription>
        Sign up with one of the following providers
      </CardDescription>
    </CardHeader>
    <CardContent>
      <form onsubmit={handleSubmit}>
        <div class="grid gap-6">
          <div class="grid gap-6">
            <div class="grid gap-3">
              <Label for="name">Name</Label>
              <Input
                id="name"
                name="name"
                type="text"
                placeholder="John Doe"
                required
              />
            </div>
            <div class="grid gap-3">
              <Label for="email">Email</Label>
              <Input
                id="email"
                type="email"
                name="email"
                placeholder="you@example.com"
                required
              />
            </div>
            <div class="grid gap-3">
              <Label for="password">Password</Label>
              <Input
                id="password"
                type="password"
                name="password"
                placeholder="********"
                required
              />
            </div>
            <div class="grid gap-3">
              <Label for="confirmPassword">Confirm password</Label>
              <Input
                id="confirmPassword"
                type="password"
                name="confirmPassword"
                required
              />
            </div>
            <Button type="submit" class="w-full">Sign up</Button>
          </div>
          <div class="text-center text-sm">
            Already have an account?{" "}
            <a href="/auth/sign-in" class="underline underline-offset-4">
              Sign in
            </a>
          </div>
        </div>
      </form>
    </CardContent>
  </Card>
</div>
