import type { ReactNode } from "react";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AppSidebar } from "@/components/app-sidebar";
import { SiteHeader } from "@/components/site-header";
import { GlobalProgressBar } from "@/components/GlobalProgressBar";
import { useLiveUpdates } from "../lib/useLiveUpdates";

export function AppLayout({ children }: { children: ReactNode }) {
  const { connected } = useLiveUpdates();

  return (
    <TooltipProvider>
      <SidebarProvider>
        <GlobalProgressBar />
        <AppSidebar />
        <SidebarInset>
          <SiteHeader streaming={connected} />
          <div className="mx-auto w-full max-w-[1600px] px-5 py-6 md:px-6">{children}</div>
        </SidebarInset>
      </SidebarProvider>
    </TooltipProvider>
  );
}
