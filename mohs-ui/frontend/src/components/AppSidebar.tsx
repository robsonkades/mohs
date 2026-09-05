import type { ComponentProps } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarFooter,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { IconActivity, IconClock, IconGauge, IconListChecks, IconServer, IconTicks } from "@/components/Icons";

export const NAV_ITEMS = [
  { to: "/", label: "Overview", icon: IconGauge, subtitle: "Cluster health at a glance" },
  { to: "/jobs", label: "Jobs", icon: IconListChecks, subtitle: "Definitions, schedules and state" },
  { to: "/executions", label: "Executions", icon: IconActivity, subtitle: "Every attempt, past and in flight" },
  { to: "/rate-limits", label: "Rate Limits", icon: IconClock, subtitle: "Throughput caps per window" },
  { to: "/runners", label: "Runners", icon: IconServer, subtitle: "Thread pools on this node" },
] as const;

export function activeNavItem(pathname: string) {
  return NAV_ITEMS.find((item) => (item.to === "/" ? pathname === "/" : pathname.startsWith(item.to)));
}

export function AppSidebar(props: ComponentProps<typeof Sidebar>) {
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const { setOpenMobile } = useSidebar();

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader className="h-18 justify-center border-b">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild className="data-[slot=sidebar-menu-button]:p-1.5!">
              <Link to="/" onClick={() => setOpenMobile(false)} aria-label="Mohs overview">
                <span className="flex size-6 shrink-0 items-center justify-center rounded bg-primary/15 text-primary">
                  <IconTicks className="size-3.5" />
                </span>
                <span className="text-base font-semibold tracking-tight">Mohs</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup className="pt-4">
          <SidebarGroupLabel className="mb-2 text-[10px] uppercase tracking-widest">Workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map(({ to, label, icon: Icon }) => {
                const isActive = to === "/" ? pathname === "/" : pathname.startsWith(to);
                return (
                  <SidebarMenuItem key={to}>
                    <SidebarMenuButton asChild isActive={isActive} tooltip={label} className="h-10 transition-colors data-[active=true]:bg-primary/10 data-[active=true]:text-primary">
                      <Link to={to} onClick={() => setOpenMobile(false)} aria-current={isActive ? "page" : undefined}>
                        <Icon />
                        <span>{label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="border-t p-4 text-xs text-muted-foreground group-data-[collapsible=icon]:hidden">
        <span className="font-medium text-foreground">Mohs scheduler</span>
        <span>Press <kbd className="rounded border px-1 font-mono">/</kbd> to find a page or job</span>
      </SidebarFooter>
    </Sidebar>
  );
}
