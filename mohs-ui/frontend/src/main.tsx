import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { router } from "./router";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
      // Refetching on focus is React Query's answer to "the tab was away and the data may be
      // old". This app answers that itself, in useLiveUpdates: coming back to the tab reopens the
      // stream AND invalidates once. Leaving both on means every alt-tab fires two rounds of the
      // same requests — the second one against data the first is still fetching.
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
