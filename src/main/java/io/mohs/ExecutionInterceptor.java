package io.mohs;

/**
 * Chain of Responsibility (GoF) para envolver a execução do handler na
 * thread da própria tentativa — lugar de MDC, spans de tracing e contexto
 * via {@code ScopedValue}. Ao contrário de {@link ExecutionListener},
 * exceção de interceptor É falha da tentativa e segue o fluxo normal de
 * retry: quem está no caminho crítico participa do desfecho.
 */
public interface ExecutionInterceptor {

    void intercept(JobContext ctx, Chain chain) throws Exception;

    /** Continuação da cadeia de interceptors até o handler. */
    interface Chain {
        void proceed() throws Exception;
    }
}
