import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Dashboard for Mohs' operational REST API (default mohs.api.base-path = "/api/mohs/v1").
// The build output is copied onto mohs-ui's classpath by Maven (see mohs-ui/pom.xml), and Spring
// Boot serves it as a static resource under /mohs-ui, on the same origin as the API.
export default defineConfig({
  base: "/mohs-ui/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: [
      // find must require the trailing slash — a bare "@" key prefix-matches Vite's own internal
      // specifiers ("@vite/client", "@react-refresh"), which breaks HMR.
      { find: /^@\//, replacement: path.resolve(import.meta.dirname, "./src") + "/" },
    ],
  },
  server: {
    proxy: {
      // Covers /overview/stream too: Vite's proxy forwards SSE without buffering, so a dashboard
      // running under `npm run dev` receives frames exactly as it would when served from the jar.
      "/api/mohs": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
