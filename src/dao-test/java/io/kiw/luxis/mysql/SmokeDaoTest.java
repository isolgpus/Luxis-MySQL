package io.kiw.luxis.mysql;

import io.kiw.luxis.result.Result;
import io.kiw.luxis.web.Luxis;
import io.vertx.sqlclient.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SmokeDaoTest extends MysqlDaoTestBase {

    @Before
    public void resetSchema() throws Exception {
        new LuxisMysqlClient(pool())
                .update("DROP TABLE IF EXISTS widget", Tuple.tuple())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        new LuxisMysqlClient(pool())
                .update("CREATE TABLE widget (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)", Tuple.tuple())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    public void insertAndSelectInTransactionCommits() throws Exception {
        final MySqlTransactionManager tm = new MySqlTransactionManager(pool());

        final MysqlTransaction tx = tm.begin().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        try {
            final LuxisMysqlClient txClient = new LuxisMysqlClient(tx.connection());
            final Integer rowsInserted = unwrap(txClient
                    .<Object>update("INSERT INTO widget (id, name) VALUES (?, ?)", Tuple.of(1L, "alpha"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(Integer.valueOf(1), rowsInserted);
            tm.commit(tx).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (final Exception e) {
            tm.rollback(tx).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            throw e;
        }

        final List<String> names = unwrap(new LuxisMysqlClient(pool())
                .<String, Object>query(
                        "SELECT name FROM widget WHERE id = ?",
                        Tuple.of(1L),
                        row -> row.getString("name"))
                .toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertEquals(List.of("alpha"), names);
    }

    @Test
    public void rollbackDiscardsWrites() throws Exception {
        final MySqlTransactionManager tm = new MySqlTransactionManager(pool());

        final MysqlTransaction tx = tm.begin().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        unwrap(new LuxisMysqlClient(tx.connection())
                .<Object>update("INSERT INTO widget (id, name) VALUES (?, ?)", Tuple.of(2L, "beta"))
                .toCompletableFuture().get(5, TimeUnit.SECONDS));
        tm.rollback(tx).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        final List<Long> ids = unwrap(new LuxisMysqlClient(pool())
                .<Long, Object>query(
                        "SELECT id FROM widget WHERE id = ?",
                        Tuple.of(2L),
                        row -> row.getLong("id"))
                .toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertEquals(List.of(), ids);
    }

    private static <T> T unwrap(final Result<?, T> result) {
        return result.fold(
                err -> {
                    throw new AssertionError("Expected success, got error: " + err);
                },
                ok -> ok);
    }
}
