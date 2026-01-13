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
  import { goto } from "$app/navigation";
  import { toast } from "svelte-sonner";
  import { betterAuthClient } from "$lib/auth-client";

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    const target = e.currentTarget as HTMLFormElement;
    const formData = new FormData(target);
    const email = formData.get("email") as string;
    const password = formData.get("password") as string;
    await betterAuthClient.signIn.email(
      { email, password },
      {
        onSuccess: async () => {
          goto("/");
        },
        onError: (error) => {
          toast.error("Error", {
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
      <CardTitle class="text-xl">Welcome back</CardTitle>
      <CardDescription>Sign in with your email and password</CardDescription>
    </CardHeader>
    <CardContent>
      <form onsubmit={handleSubmit}>
        <div class="grid gap-6">
          <div class="grid gap-6">
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
              <div class="flex items-center justify-between">
                <Label for="password">Password</Label>
                <button
                  onclick={() => toast.error("Not implemented in this demo")}
                  class="ml-auto inline-block text-sm underline-offset-4 hover:underline"
                >
                  Forgot your password?
                </button>
              </div>
              <Input id="password" type="password" name="password" required />
            </div>
            <Button type="submit" class="w-full">Sign in</Button>
          </div>
          <div class="text-center text-sm">
            Don&apos;t have an account?{" "}
            <a href="/auth/sign-up" class="underline underline-offset-4">
              Sign up
            </a>
          </div>
        </div>
      </form>
    </CardContent>
  </Card>
</div>
