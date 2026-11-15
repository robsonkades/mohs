/*
 * Copyright 2026 The Mohs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mohs.autoconfigure;

import java.io.IOException;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnResource;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;
import org.springframework.web.servlet.resource.ResourceTransformer;
import org.springframework.web.servlet.resource.ResourceTransformerChain;
import org.springframework.web.servlet.resource.TransformedResource;
import org.springframework.web.util.HtmlUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import tools.jackson.databind.json.JsonMapper;

/**
 * Serves the {@code mohs-ui} dashboard when it is on the classpath.
 *
 * <p>The condition is the bundle itself ({@code @ConditionalOnResource} over {@code index.html}),
 * not a marker class: {@code mohs-ui} is a resource-only jar and the starter does not depend on it
 * — whoever wants the dashboard declares {@code io.mohs:mohs-ui} and it appears. Without the jar
 * nothing here activates, and the application does not even pay for evaluating a bean.
 *
 * <p>It runs on the host application's own server, deliberately as the only mode: a server of ours
 * would sit entirely outside the host's Spring Security filter chain, and an application that
 * protected itself carefully would still expose pause/cancel/retry on a side port. On the host's
 * server, the host's security configuration applies — protect the {@code mohs.api.base-path} prefix
 * and {@code /mohs-ui} there.
 */
@AutoConfiguration
@ConditionalOnClass(DispatcherServlet.class)
@ConditionalOnProperty(prefix = "mohs", name = "enabled", matchIfMissing = true)
@ConditionalOnResource(resources = "classpath:/mohs-ui-webapp/index.html")
public class MohsUiAutoConfiguration {

    /**
     * Creates a {@code MohsUiAutoConfiguration} instance.
     */
    public MohsUiAutoConfiguration() {
    }

    private static final Logger log = LoggerFactory.getLogger(MohsUiAutoConfiguration.class);

    /** Where the dashboard is mounted. It matches Vite's {@code base} and the router's basepath. */
    static final String UI_PATH = "/mohs-ui";

    private static final String WEBAPP_LOCATION = "classpath:/mohs-ui-webapp/";

    private static final String INDEX = "/mohs-ui-webapp/index.html";

    private static final String FORWARD_TO_INDEX_VIEW = "forward:" + UI_PATH + "/index.html";

    /**
     * A classpath location of its own rather than one of Boot's default static directories (which
     * map to {@code /}): that way the dashboard never collides with what the host already serves at
     * the root — an actuator with {@code base-path: /}, for instance.
     *
     * <p>The bare mount path ({@code /mohs-ui} and {@code /mohs-ui/}) needs the explicit forward
     * below, separate from {@link SpaFallbackResourceResolver}'s fallback:
     * {@link ResourceHttpRequestHandler} returns 404 for an empty resource path before any
     * {@link PathResourceResolver} runs, so the resolver's fallback never gets to apply to it.
     */
    @Bean
    WebMvcConfigurer mohsUiStaticAppConfigurer(MohsProperties properties) {
        warnIfDashboardHasNoApiToRead(properties);
        return new WebMvcConfigurer() {
            @Override
            public void addViewControllers(ViewControllerRegistry registry) {
                registry.addViewController(UI_PATH).setViewName(FORWARD_TO_INDEX_VIEW);
                registry.addViewController(UI_PATH + "/").setViewName(FORWARD_TO_INDEX_VIEW);
            }

            /**
             * Two handlers, because the bundle has two kinds of file with opposite caching needs.
             * Vite writes every script and stylesheet under {@code assets/} with a content hash in
             * the name, so those are immutable for a year and a missing one is a plain 404 — a
             * browser holding a stale {@code index.html} after a deploy must learn that its script
             * is gone, not receive HTML in its place and fail with an obscure syntax error. Everything
             * else ({@code index.html}, the icons, and the client routes that fall back to the page)
             * is {@code no-store}: the page includes per-request host configuration and may carry
             * a CSRF token, so it must not be reused for another session.
             * Deliberately no {@code public}: the host is told to put this path behind its own
             * authentication, and {@code public} is the one directive that lets a shared cache store
             * a response to an authenticated request — the browser's private cache honours
             * {@code max-age} and {@code immutable} without it, and a host with a CDN adds
             * {@code public} at its proxy.
             *
             * <p>{@code resourceChain(false)}: no {@code CachingResourceResolver}. It is a
             * {@code ConcurrentMapCache} with neither TTL nor ceiling, keyed by the request path —
             * and the fallback makes EVERY non-asset path under {@code /mohs-ui/**} resolve
             * successfully, including nonexistent ones. With no 404, the valve that normally keeps
             * the cache from growing disappears: a crawler hitting random paths would create a
             * permanent entry per path, for the life of the process. And nothing is lost in
             * exchange — the resources are immutable inside the jar, and resolving them through the
             * classloader is already cheap.
             */
            @Override
            public void addResourceHandlers(ResourceHandlerRegistry registry) {
                registry.addResourceHandler(UI_PATH + "/assets/**")
                        .addResourceLocations(WEBAPP_LOCATION + "assets/")
                        .setCacheControl(CacheControl.maxAge(Duration.ofDays(365)).immutable())
                        .resourceChain(false);
                registry.addResourceHandler(UI_PATH + "/**")
                        .addResourceLocations(WEBAPP_LOCATION)
                        .setCacheControl(CacheControl.noStore())
                        .setUseLastModified(false)
                        .resourceChain(false)
                        .addResolver(new SpaFallbackResourceResolver())
                        .addTransformer(new DashboardConfigurationTransformer(properties));
            }
        };
    }

    /**
     * The dashboard consumes the API but is not conditioned on it. Warn when the host disables that
     * API; a custom base path is supported through the per-request bootstrap configuration.
     */
    private static void warnIfDashboardHasNoApiToRead(MohsProperties properties) {
        if (!properties.api().enabled()) {
            log.warn("mohs-ui is on the classpath but mohs.api.enabled=false — the dashboard will load and stay"
                    + " empty, because it has no API to read. Set mohs.api.enabled=true, or drop the"
                    + " io.mohs:mohs-ui dependency.");
        }
    }

    /** Per-request bootstrap data: the host's context, API path and optional Spring Security CSRF token. */
    private static final class DashboardConfigurationTransformer implements ResourceTransformer {
        private final MohsProperties properties;
        private final JsonMapper json = JsonMapper.builder().build();

        private DashboardConfigurationTransformer(MohsProperties properties) {
            this.properties = properties;
        }

        @Override
        public Resource transform(HttpServletRequest request, Resource resource, ResourceTransformerChain chain)
                throws IOException {
            Resource transformed = chain.transform(request, resource);
            if (!"index.html".equals(resource.getFilename())) {
                return transformed;
            }
            String uiBase = request.getContextPath() + UI_PATH;
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("apiBase", request.getContextPath() + properties.api().basePath());
            config.put("uiBase", uiBase);
            config.put("nodeLeaseTtlSeconds", properties.engine().nodeLeaseTtl().toNanos() / 1_000_000_000d);
            addCsrf(request, config);
            // Inert JSON works with a CSP that disallows inline executable scripts. Escape HTML
            // delimiters as JSON unicode escapes so a value can never terminate the script element.
            String encoded = json.writeValueAsString(config).replace("<", "\\u003c")
                    .replace(">", "\\u003e").replace("&", "\\u0026");
            String bootstrap = "<base href=\"" + HtmlUtils.htmlEscape(uiBase + "/") + "\">"
                    + "<script type=\"application/json\" id=\"mohs-config\">" + encoded + "</script>";
            String html = transformed.getContentAsString(StandardCharsets.UTF_8);
            int head = html.indexOf("<head>");
            html = head >= 0 ? html.substring(0, head + 6) + bootstrap + html.substring(head + 6)
                    : bootstrap + html;
            return new TransformedResource(transformed, html.getBytes(StandardCharsets.UTF_8));
        }

        private static void addCsrf(HttpServletRequest request, Map<String, Object> config) {
            Object csrf = request.getAttribute("org.springframework.security.web.csrf.CsrfToken");
            if (csrf == null) {
                return;
            }
            // Spring Security is owned by the host and remains an optional dependency. Invoking
            // its public interface also supports the deferred token wrappers installed by its filter.
            Class<?> contract = ClassUtils.resolveClassName("org.springframework.security.web.csrf.CsrfToken",
                    csrf.getClass().getClassLoader());
            if (contract.isInstance(csrf)) {
                config.put("csrfHeaderName", ReflectionUtils.invokeMethod(
                        java.util.Objects.requireNonNull(ReflectionUtils.findMethod(contract, "getHeaderName")), csrf));
                config.put("csrfToken", ReflectionUtils.invokeMethod(
                        java.util.Objects.requireNonNull(ReflectionUtils.findMethod(contract, "getToken")), csrf));
            }
        }
    }

    /**
     * A sub-path that is not a real file falls back to {@code index.html}, so that refreshing on a
     * client route ({@code /mohs-ui/jobs}) resolves instead of 404ing — the router mounted at that
     * same basepath then renders it. The rule is by location, not by the shape of the path: a
     * client route can carry a dot ({@code /mohs-ui/jobs/welcome.email} is a job key), so "has an
     * extension" would be the wrong test — the hashed scripts live under {@code assets/}, which has
     * its own handler and never reaches this resolver.
     */
    private static final class SpaFallbackResourceResolver extends PathResourceResolver {

        /**
         * {@code checkResource} comes from the supertype and is what guarantees the resolved
         * resource is still INSIDE the location — the defence against {@code ../} and symlink
         * escape. Overriding {@code getResource} without reintroducing it would leave only
         * {@code ResourceHttpRequestHandler}'s {@code isInvalidPath} standing; overriding is not
         * reimplementing from scratch.
         *
         * <p>A resource that does not pass falls back to {@code index.html} together with one that
         * simply does not exist: to the caller, "not an asset of this application" is the same
         * answer.
         */
        @Override
        protected Resource getResource(String resourcePath, Resource location) throws IOException {
            Resource requested = location.createRelative(resourcePath);
            boolean servable = requested.exists() && requested.isReadable() && checkResource(requested, location);
            return servable ? requested : new ClassPathResource(INDEX);
        }
    }

}
