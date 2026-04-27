package io.kiw.luxis.mysql;

import io.kiw.luxis.result.Result;
import io.kiw.luxis.web.Luxis;
import io.kiw.luxis.web.TestLuxis;
import io.kiw.luxis.web.TransactionManager;
import io.kiw.luxis.web.http.HttpErrorResponse;
import io.kiw.luxis.web.http.HttpResult;
import io.kiw.luxis.web.http.HttpSession;
import io.kiw.luxis.web.http.Method;
import io.kiw.luxis.web.http.client.LuxisAsync;
import io.kiw.luxis.web.pipeline.LuxisStream;
import io.kiw.luxis.web.test.StubRequest;
import io.kiw.luxis.web.test.StubTestClient;
import io.kiw.luxis.web.test.TestHttpResponse;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SmokeDaoTest extends MysqlDaoTestBase {

    @Before
    public void resetSchema() throws Exception {
        runUpdate("DROP TABLE IF EXISTS widget");
        runUpdate("CREATE TABLE widget (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
        runUpdate("DROP TABLE IF EXISTS users");
        runUpdate("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
    }

    private static void runUpdate(final String sql) throws Exception {
        new LuxisMysqlClient(pool())
                .update(sql, Tuple.tuple())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }


    @Test
    public void httpStreamCreatesUserWithinTransaction() throws Exception {
        final Pool pool = pool();
        final TrackingTransactionManager tm = new TrackingTransactionManager(new MySqlTransactionManager(pool));

        final LuxisMysqlClient luxisMysqlClient = new LuxisMysqlClient(pool);
        try (TestLuxis<Void> luxis = Luxis.test(routes -> {
            routes.jsonRoute("/users", Method.POST, null, CreateUserRequest.class,
                    stream -> {
                        final LuxisStream<CreateUserResponse, Object, Void, HttpErrorResponse, HttpSession> transactionStream = stream
                                .inTransaction(tx -> {
                                    return tx
                                            .asyncMap(ctx -> {
                                                final LuxisAsync<Integer, HttpErrorResponse> update = luxisMysqlClient
                                                        .update(
                                                                "INSERT INTO users (id, name) VALUES (?, ?)",
                                                                Tuple.of(ctx.in().id, ctx.in().name));
                                                final LuxisAsync<CreateUserResponse, HttpErrorResponse> map = update
                                                        .map(rows -> new CreateUserResponse(ctx.in().id, ctx.in().name));
                                                return map;
                                            })
                                            .commit();
                                });
                        return transactionStream
                                .complete(ctx -> HttpResult.success(ctx.in()));
                    });
            return null;
        }, tm)) {
            luxis.setExceptionHandler(Throwable::printStackTrace);
            final StubTestClient client = new StubTestClient("localhost", 0, luxis);
            luxis.setExceptionHandler(Throwable::printStackTrace);

            final TestHttpResponse response = client.post(
                    StubRequest.request("/users").body("{\"id\":42,\"name\":\"alice\"}"));

            client.assertNoMoreExceptions();
            assertEquals(response.responseBody, 200, response.statusCode);
            assertEquals("{\"id\":42,\"name\":\"alice\"}", response.responseBody);

            final List<String> committedNames = unwrap(luxisMysqlClient
                    .<String, Object>query(
                            "SELECT name FROM users WHERE id = ?",
                            Tuple.of(42L),
                            row -> row.getString("name"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(List.of("alice"), committedNames);

            client.assertNoMoreExceptions();
        }
    }

    public static final class CreateUserRequest {
        public long id;
        public String name;
    }

    public static final class CreateUserResponse {
        public long id;
        public String name;

        public CreateUserResponse() {
        }

        public CreateUserResponse(final long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static final class TrackingTransactionManager implements TransactionManager<MysqlTransaction> {
        private static final ThreadLocal<MysqlTransaction> CURRENT = new ThreadLocal<>();

        private final MySqlTransactionManager delegate;

        TrackingTransactionManager(final MySqlTransactionManager delegate) {
            this.delegate = delegate;
        }

        static MysqlTransaction current() {
            final MysqlTransaction tx = CURRENT.get();
            if (tx == null) {
                throw new IllegalStateException("No active MySQL transaction on this thread");
            }
            return tx;
        }

        @Override
        public Future<MysqlTransaction> begin() {
            return delegate.begin().onSuccess(CURRENT::set);
        }

        @Override
        public Future<Void> commit(final MysqlTransaction tx) {
            return delegate.commit(tx).eventually(() -> {
                CURRENT.remove();
                return Future.succeededFuture();
            });
        }

        @Override
        public Future<Void> rollback(final MysqlTransaction tx) {
            return delegate.rollback(tx).eventually(() -> {
                CURRENT.remove();
                return Future.succeededFuture();
            });
        }
    }

    private static <T> T unwrap(final Result<?, T> result) {
        return result.fold(
                err -> {
                    throw new AssertionError("Expected success, got error: " + err);
                },
                ok -> ok);
    }
}
