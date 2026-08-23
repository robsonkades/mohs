import type { ReactNode } from "react";
import { useRouterState } from "@tanstack/react-router";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AppSidebar } from "@/components/AppSidebar";
import { SiteHeader } from "@/components/SiteHeader";
import { GlobalProgressBar } from "@/components/GlobalProgressBar";
import { useLiveUpdates } from "../lib/useLiveUpdates";
import { ErrorBoundary } from "@/components/ErrorBoundary";

export function AppLayout({ children }: { children: ReactNode }) {
  const { status } = useLiveUpdates();
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  return (
    <TooltipProvider>
      <SidebarProvider>
        <GlobalProgressBar />
        <AppSidebar />
        <SidebarInset>
          <SiteHeader streamStatus={status} />
          <div className="mx-auto w-full max-w-[1600px] px-5 py-6 md:px-6">
            <ErrorBoundary resetKey={pathname}>{children}</ErrorBoundary>
          </div>
        </SidebarInset>
      </SidebarProvider>
    </TooltipProvider>
  );
}
