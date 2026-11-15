export interface MohsUiConfig {
  apiBase: string;
  uiBase: string;
  csrfHeaderName?: string;
  csrfToken?: string;
  nodeLeaseTtlSeconds: number;
}

declare global { interface Window { __MOHS_CONFIG__?: Partial<MohsUiConfig> } }

function readConfig(): Partial<MohsUiConfig> {
  if (typeof document === "undefined") return {};
  const text = document.getElementById("mohs-config")?.textContent;
  let embedded: Partial<MohsUiConfig> = {};
  if (text) {
    try { embedded = JSON.parse(text) as Partial<MohsUiConfig>; }
    catch { throw new Error("The dashboard configuration could not be read. Reload or contact the application operator."); }
  }
  return { ...embedded, ...window.__MOHS_CONFIG__ };
}

const configured = readConfig();
export const runtimeConfig: MohsUiConfig = {
  apiBase: configured.apiBase ?? "/api/mohs/v1",
  uiBase: configured.uiBase ?? "/mohs-ui",
  csrfHeaderName: configured.csrfHeaderName,
  csrfToken: configured.csrfToken,
  nodeLeaseTtlSeconds: configured.nodeLeaseTtlSeconds ?? 15,
};
