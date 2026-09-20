import { useEffect, useState } from "react";
import { useDocumentVisible } from "./useDocumentVisible";

const minute = () => Math.floor(Date.now() / 60_000) * 60_000;

/** Live windows advance by a minute. Paused/history pages retain their original cursor interval. */
export function useRollingAnchor(live: boolean): number {
  const visible = useDocumentVisible();
  const [anchor, setAnchor] = useState(minute);
  useEffect(() => {
    if (!live || !visible) return;
    const advance = () => setAnchor(minute());
    advance();
    const timer = window.setInterval(advance, 10_000);
    window.addEventListener("mohs:refresh", advance);
    return () => { window.clearInterval(timer); window.removeEventListener("mohs:refresh", advance); };
  }, [live, visible]);
  return anchor;
}
