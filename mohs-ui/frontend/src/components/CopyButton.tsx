import { useState } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { Button } from "@/components/ui/button";

/** One-click copy for the mono ids the UI truncates — the check flash is the only feedback needed. */
export function CopyButton({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false);

  return (
    <Button
      variant="ghost"
      size="icon-xs"
      onClick={(e) => {
        e.stopPropagation();
        navigator.clipboard?.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
      aria-label={label ?? "Copy to clipboard"}
      title={label ?? "Copy to clipboard"}
    >
      {copied ? <CheckIcon className="text-good" /> : <CopyIcon />}
    </Button>
  );
}
