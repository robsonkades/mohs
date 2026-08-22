package io.mohs.store.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.mohs.engine.StoreTransactions;

/**
 * {@link StoreTransactions} sobre o {@code DataSource} do Mohs —
 * propagação {@code NESTED} de propósito, o oposto exato de
 * claim/conclusão: sem transação ativa comporta-se como {@code REQUIRED}
 * (abre e commita a própria); DENTRO da transação do host (ADR-0003 §4,
 * "joins your transaction") vira <b>savepoint</b> — e é o savepoint que
 * torna o Idempotent Receiver composable: o conflito de PK de
 * {@code mohs_idempotency} desfaz SÓ a unidade de enqueue, deixando a
 * conexão sã (no Postgres, violação sem savepoint aborta a transação
 * inteira — {@code 25P02} — e o caminho de recuperação que lê o vencedor
 * ficaria inalcançável) e a transação do host commitável (com
 * {@code REQUIRED}, o rollback-only do template condenaria o commit do
 * host DEPOIS de devolvermos um {@code Enqueued} de sucesso). A semântica
 * "junta-se e cai junto" é preservada: a unidade só fica durável com o
 * commit do host. Sem isolação explícita — dentro da transação do host, a
 * isolação é dele; inserts puros não dependem de nível.
 */
public final class JdbcStoreTransactions implements StoreTransactions {

    private final TransactionTemplate transactionTemplate;

    public JdbcStoreTransactions(DataSource dataSource) {
        // DataSourceTransactionManager nasce com nestedTransactionAllowed=true —
        // NESTED aqui é savepoint JDBC puro, suportado pelos 4 dialetos
        this.transactionTemplate = new TransactionTemplate(
                new DataSourceTransactionManager(Objects.requireNonNull(dataSource, "dataSource")));
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    @Override
    public void inTransaction(Runnable work) {
        Objects.requireNonNull(work, "work");
        transactionTemplate.executeWithoutResult(_ -> work.run());
    }
}
