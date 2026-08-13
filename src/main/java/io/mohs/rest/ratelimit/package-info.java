/**
 * Área de recurso "rate-limits": {@link io.mohs.rest.ratelimit.RateLimitsController}
 * ({@code GET}/{@code PATCH /rate-limits/{name}}, cluster-wide) e seus
 * DTOs — {@link io.mohs.rest.ratelimit.RateLimitResponse},
 * {@link io.mohs.rest.ratelimit.RateLimitPatchRequest}. Mesmo contrato de
 * {@code io.mohs.rest.queue}, pra vazão em vez de concorrência. Depende
 * de {@code io.mohs.rest} ({@code RuntimePatchResponse}).
 */
@NullMarked
package io.mohs.rest.ratelimit;

import org.jspecify.annotations.NullMarked;
