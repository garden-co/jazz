"use client";
import { PosterShopApp } from "../../src/App.js";
export default function Dashboard() {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  if (!appId || !serverUrl)
    return <main>Configure NEXT_PUBLIC_JAZZ_APP_ID and NEXT_PUBLIC_JAZZ_SERVER_URL.</main>;
  // Hosts obtain the JWT from their Better Auth session endpoint and pass it here.
  return <main>PosterShop requires an authenticated Better Auth session.</main>;
}
void PosterShopApp;
