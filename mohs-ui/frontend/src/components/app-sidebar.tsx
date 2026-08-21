import type { ComponentProps } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { IconActivity, IconClock, IconGauge, IconListChecks, IconServer, IconTicks } from "@/components/icons";

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

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild className="data-[slot=sidebar-menu-button]:p-1.5!">
              <Link to="/">
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
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map(({ to, label, icon: Icon }) => {
                const isActive = to === "/" ? pathname === "/" : pathname.startsWith(to);
                return (
                  <SidebarMenuItem key={to}>
                    <SidebarMenuButton asChild isActive={isActive} tooltip={label}>
                      <Link to={to}>
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
    </Sidebar>
  );
}
