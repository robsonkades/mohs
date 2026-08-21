import type { InputHTMLAttributes, ReactNode } from "react";
import { Input } from "@/components/ui/input";

export function TextInput(props: InputHTMLAttributes<HTMLInputElement>) {
  return <Input {...props} />;
}

export function FilterBar({ children }: { children: ReactNode }) {
  return <div className="mb-4 flex flex-wrap items-center gap-2.5">{children}</div>;
}
