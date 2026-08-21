import { useEffect, useState } from "react";
import { useRouterState } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { RefreshControls } from "@/components/refresh-controls";
import { CommandPalette } from "@/components/CommandPalette";
import { IconSearch } from "@/components/icons";
import { activeNavItem } from "@/components/app-sidebar";

export function SiteHeader({ streaming }: { streaming: boolean }) {
  const [paletteOpen, setPaletteOpen] = useState(false);
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const currentItem = activeNavItem(pathname);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const typing = /^(input|textarea|select)$/i.test((e.target as HTMLElement)?.tagName ?? "");
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((open) => !open);
      } else if (e.key === "/" && !typing) {
        e.preventDefault();
        setPaletteOpen(true);
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  return (
    <header className="sticky top-0 z-30 flex h-14 shrink-0 items-center gap-3 border-b bg-background/80 px-4 backdrop-blur-md lg:px-6">
      <SidebarTrigger />
      <Separator orientation="vertical" className="h-4" />
      <div className="flex min-w-0 flex-col">
        <h1 className="text-sm font-semibold leading-tight">{currentItem?.label}</h1>
        {currentItem?.subtitle && (
          <p className="truncate text-xs leading-tight text-muted-foreground">{currentItem.subtitle}</p>
        )}
      </div>
      <Button
        variant="outline"
        onClick={() => setPaletteOpen(true)}
        className="ml-3 hidden gap-2 text-xs text-muted-foreground md:flex"
        aria-label="Open global search"
      >
        <IconSearch className="size-3.5" />
        Search
        <kbd className="rounded-sm border bg-background px-1 font-mono text-[10px]">/</kbd>
      </Button>
      <RefreshControls streaming={streaming} />

      <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} />
    </header>
  );
}
