import { useEffect, useRef, useState } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { Button } from "@/components/ui/button";

/** One-click copy for the mono ids the UI truncates — the check flash is the only feedback needed. */
export function CopyButton({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);

  return (
    <span className="inline-flex shrink-0 items-center">
    <Button
      variant="ghost"
      size="icon-xs"
      onClick={async (e) => {
        e.stopPropagation();
        clearTimeout(timer.current);
        setFailed(false);
        setCopied(false);
        try {
          if (!navigator.clipboard) throw new Error("Clipboard unavailable");
          await navigator.clipboard.writeText(value);
          setCopied(true);
          timer.current = setTimeout(() => setCopied(false), 1500);
        } catch {
          setFailed(true);
        }
      }}
      aria-label={label ?? "Copy to clipboard"}
      title={label ?? "Copy to clipboard"}
    >
      {copied ? <CheckIcon className="text-good" /> : <CopyIcon />}
    </Button>
    <span role="status" className={failed ? "max-w-40 whitespace-normal text-xs text-critical" : "sr-only"}>
      {failed ? "Could not copy. Select and copy the text manually." : copied ? "Copied to clipboard" : ""}
    </span>
    </span>
  );
}
