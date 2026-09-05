import { useEffect, useState } from "react";
import { useRouterState } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { RefreshControls } from "@/components/RefreshControls";
import { CommandPalette } from "@/components/CommandPalette";
import { IconSearch } from "@/components/Icons";
import { activeNavItem } from "@/components/AppSidebar";
import type { StreamStatus } from "@/lib/useLiveUpdates";

export function SiteHeader({ streamStatus }: { streamStatus: StreamStatus }) {
  const [paletteOpen, setPaletteOpen] = useState(false);
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const currentItem = activeNavItem(pathname);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const target = e.target instanceof HTMLElement ? e.target : null;
      const typing = target?.isContentEditable || !!target?.closest("input, textarea, select, [role='textbox']");
      if (e.isComposing || e.defaultPrevented || e.repeat) return;
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((open) => !open);
      } else if (e.key === "/" && !typing && !e.altKey && !e.ctrlKey && !e.metaKey && !document.querySelector('[role="dialog"]')) {
        e.preventDefault();
        setPaletteOpen(true);
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  return (
    <header className="sticky top-0 z-30 flex h-18 shrink-0 items-center gap-2 border-b bg-background/90 px-3 backdrop-blur-md sm:gap-3 lg:px-6">
      <SidebarTrigger />
      <Separator orientation="vertical" className="h-4" />
      <div className="flex min-w-0 flex-col">
        <h1 className="text-base font-semibold leading-tight tracking-tight sm:text-lg">{currentItem?.label}</h1>
        {currentItem?.subtitle && (
          <p className="mt-1 hidden truncate text-xs leading-tight text-muted-foreground sm:block">{currentItem.subtitle}</p>
        )}
      </div>
      <Button
        variant="outline"
        onClick={() => setPaletteOpen(true)}
        className="ml-1 shrink-0 gap-2 text-xs text-muted-foreground md:ml-5 md:w-48 md:justify-start"
        aria-label="Open global search"
      >
        <IconSearch className="size-3.5" />
        <span className="hidden md:inline">Search pages & jobs</span>
        <kbd className="ml-auto hidden rounded-sm border bg-background px-1 font-mono text-[10px] md:inline">/</kbd>
      </Button>
      <RefreshControls streamStatus={streamStatus} />

      <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} />
    </header>
  );
}
