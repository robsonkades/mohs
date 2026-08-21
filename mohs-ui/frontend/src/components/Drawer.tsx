import type { ReactNode } from "react";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";

export function Drawer({
  open,
  title,
  onClose,
  children,
}: {
  open: boolean;
  title: ReactNode;
  onClose: () => void;
  children: ReactNode;
}) {
  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent className="w-full gap-0 sm:max-w-[480px]">
        <SheetHeader className="border-b">
          <SheetTitle className="text-headline">{title}</SheetTitle>
        </SheetHeader>
        <div className="flex-1 overflow-y-auto px-4 pb-5">{children}</div>
      </SheetContent>
    </Sheet>
  );
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-1 py-2.5">
      <span className="mono-label text-muted-foreground">{label}</span>
      <div className="text-sm">{children}</div>
    </div>
  );
}
