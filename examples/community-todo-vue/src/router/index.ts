import { createRouter, createWebHistory } from "vue-router";
import Home from "../views/Home.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/",
      name: "Home",
      component: Home,
    },
    {
      path: "/project/:projectId",
      name: "Project",
      component: () => import("../views/ProjectView.vue"),
      props: true,
    },
    {
      path: "/invite/:path*",
      name: "AcceptInvite",
      component: () => import("../views/AcceptInviteView.vue"),
    },
  ],
});

export default router;
