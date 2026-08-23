import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { IconAlertTriangle } from "./Icons";
import { Panel } from "./Panel";

interface Props {
  /** Changing this discards the error — the router path, so navigating away recovers. */
  resetKey: string;
  children: ReactNode;
}

interface State {
  error: Error | null;
  componentStack: string | null;
}

/**
 * The last line of defence for a render that throws.
 *
 * <p>Without one, a single bad render unmounts the entire tree and leaves a white page — and in a
 * production bundle the only clue is a minified stack like `at dl (index-abc.js:9:98072)`, which
 * names nothing. React hands `componentInfo.componentStack` to `componentDidCatch` even in
 * production builds, so catching the error is what turns "the dashboard went blank" into a
 * report that says which component was rendering.
 *
 * <p>Scoped to the page content, not the whole app: the sidebar and header keep working, so the
 * operator can navigate somewhere else instead of reloading. `NotFoundError: removeChild` — the
 * classic symptom of the real DOM being mutated under React by a browser extension or by page
 * translation — lands here too, and degrades to a panel rather than a blank screen.
 */
export class ErrorBoundary extends Component<Props, State> {
  override state: State = { error: null, componentStack: null };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  override componentDidCatch(error: Error, info: ErrorInfo): void {
    this.setState({ componentStack: info.componentStack ?? null });
    console.error("Dashboard render failed", error, info.componentStack);
  }

  override componentDidUpdate(previous: Props): void {
    if (previous.resetKey !== this.props.resetKey && this.state.error !== null) {
      this.setState({ error: null, componentStack: null });
    }
  }

  override render(): ReactNode {
    const { error, componentStack } = this.state;
    if (error === null) {
      return this.props.children;
    }

    return (
      <Panel title="This page stopped rendering">
        <div className="flex flex-col gap-4 py-4">
          <div className="flex items-start gap-3">
            <IconAlertTriangle className="mt-0.5 size-5 shrink-0 text-critical" />
            <div className="flex min-w-0 flex-col gap-1">
              <p className="text-sm font-medium">{error.message}</p>
              <p className="text-xs text-muted-foreground">
                Navigating to another page clears this. If it keeps happening, the component stack
                below says where it started.
              </p>
            </div>
          </div>
          {componentStack && (
            <pre className="max-h-64 overflow-auto rounded-lg border bg-page p-3 font-mono text-[11px] leading-relaxed text-muted-foreground">
              {componentStack.trim()}
            </pre>
          )}
          <div>
            <Button variant="outline" size="sm" onClick={() => window.location.reload()}>
              Reload the dashboard
            </Button>
          </div>
        </div>
      </Panel>
    );
  }
}
