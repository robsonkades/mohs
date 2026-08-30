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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnResource;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import io.mohs.rest.ApiPaths;

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
             * {@code resourceChain(false)}: no {@code CachingResourceResolver}. It is a
             * {@code ConcurrentMapCache} with neither TTL nor ceiling, keyed by the request path —
             * and the fallback below makes EVERY path under {@code /mohs-ui/**} resolve
             * successfully, including nonexistent ones. With no 404, the valve that normally keeps
             * the cache from growing disappears: a crawler hitting random paths would create a
             * permanent entry per path, for the life of the process. And nothing is lost in
             * exchange — the resources are immutable inside the jar, and resolving them through the
             * classloader is already cheap.
             */
            @Override
            public void addResourceHandlers(ResourceHandlerRegistry registry) {
                registry.addResourceHandler(UI_PATH + "/**")
                        .addResourceLocations(WEBAPP_LOCATION)
                        .resourceChain(false)
                        .addResolver(new SpaFallbackResourceResolver());
            }
        };
    }

    /**
     * The dashboard consumes the API but is not conditioned on it, and {@code api.ts} pins
     * {@link ApiPaths#V1} while {@code mohs.api.base-path} is configurable. In both cases the page
     * loaded, every fetch returned 404, and there was no log at all: the classic "I opened the
     * dashboard and nothing shows up" at 3 a.m. A WARN closes the operational gap without inventing
     * a new property.
     */
    private static void warnIfDashboardHasNoApiToRead(MohsProperties properties) {
        if (!properties.api().enabled()) {
            log.warn("mohs-ui is on the classpath but mohs.api.enabled=false — the dashboard will load and stay"
                    + " empty, because it has no API to read. Set mohs.api.enabled=true, or drop the"
                    + " io.mohs:mohs-ui dependency.");
        } else if (!ApiPaths.V1.equals(properties.api().basePath())) {
            log.warn("mohs-ui is pinned to {} but mohs.api.base-path={} — the dashboard will 404 on every call."
                    + " Serve the API at the default prefix, or proxy {} to it.",
                    ApiPaths.V1, properties.api().basePath(), ApiPaths.V1);
        }
    }

    /**
     * A sub-path that is not a real asset falls back to {@code index.html}, so that refreshing on a
     * client route ({@code /mohs-ui/jobs}) resolves instead of 404ing — the router mounted at that
     * same basepath then renders it.
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
