package io.kiw.luxis.mysql;

import io.kiw.luxis.web.Luxis;
import io.kiw.luxis.web.TestLuxis;
import io.kiw.luxis.web.handler.JsonHandler;
import io.kiw.luxis.web.http.ErrorMessageResponse;
import io.kiw.luxis.web.http.ErrorStatusCode;
import io.kiw.luxis.web.http.HttpResult;
import io.kiw.luxis.web.http.Method;
import io.kiw.luxis.web.internal.LuxisPipeline;
import io.kiw.luxis.web.pipeline.HttpStream;
import io.kiw.luxis.web.test.StubRequest;
import io.kiw.luxis.web.test.StubTestClient;
import io.kiw.luxis.web.test.TestHttpResponse;
import io.vertx.sqlclient.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SmokeDaoTest extends MysqlDaoTestBase {

    private MysqlDatabaseClient databaseClient = new MysqlDatabaseClient(pool());
    private StubTestClient client;

    @Before
    public void resetSchema() throws Exception {
        runUpdate("DROP TABLE IF EXISTS widget");
        runUpdate("CREATE TABLE widget (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
        runUpdate("DROP TABLE IF EXISTS users");
        runUpdate("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
        final TestLuxis<Void> luxis = Luxis.test(routes -> {
            routes.jsonRoute("/users", Method.POST, null, CreateUserRequest.class, new CreateUserTransactionallyHandler());
            routes.jsonRoute("/users/rollback", Method.POST, null, CreateUserRequest.class, new RollbackUserHandler());
            routes.jsonRoute("/widgets", Method.POST, null, CreateWidgetRequest.class, new CreateWidgetNonTxHandler());
            routes.jsonRoute("/widgets/find", Method.POST, null, FindWidgetRequest.class, new FindWidgetNonTxHandler());
            routes.jsonRoute("/widgets/find-tx", Method.POST, null, FindWidgetRequest.class, new FindWidgetInTxHandler());
            routes.jsonRoute("/widgets/named", Method.POST, null, CreateWidgetRequest.class, new CreateWidgetNamedHandler());
            routes.jsonRoute("/widgets/batch", Method.POST, null, CreateWidgetsRequest.class, new BatchInsertWidgetsHandler());
            routes.jsonRoute("/widgets/batch-named", Method.POST, null, CreateWidgetsRequest.class, new BatchInsertWidgetsNamedHandler());
            routes.jsonRoute("/widgets/two-then-fail", Method.POST, null, CreateWidgetsRequest.class, new TwoInsertsThenFailHandler());
            routes.jsonRoute("/widgets/duplicate-pk", Method.POST, null, CreateWidgetsRequest.class, new DuplicatePkInTxHandler());
            routes.jsonRoute("/sql/named-update", Method.POST, null, NamedSqlRequest.class, new RawNamedUpdateHandler());
            return null;
        }, databaseClient);

        client = new StubTestClient("localhost", 0, luxis);
        luxis.setExceptionHandler(Throwable::printStackTrace);
    }

    private static void runUpdate(final String sql) throws Exception {
        new MysqlDatabaseClient(pool())
                .update(null, sql).toCompletionStage().toCompletableFuture().join();
    }


    @Test
    public void httpStreamCreatesUserWithinTransaction() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/users").body("{\"id\":42,\"name\":\"alice\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertEquals("{\"id\":42,\"name\":\"alice\"}", response.responseBody);

        assertEquals(List.of("alice"), selectUserNames(42L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void transactionIsRolledBackWhenChainReturnsError() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/users/rollback").body("{\"id\":99,\"name\":\"bob\"}"));

        assertEquals(500, response.statusCode);
        assertEquals(List.of(), selectUserNames(99L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void updateOutsideTransactionPersistsImmediately() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets").body("{\"id\":1,\"name\":\"sprocket\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertEquals(List.of("sprocket"), selectWidgetNames(1L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void queryOutsideTransactionReadsCommittedRows() throws Exception {
        databaseClient.update(null, "INSERT INTO widget (id, name) VALUES (?, ?)", 7L, "cog")
                .toCompletionStage().toCompletableFuture().join();

        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/find").body("{\"id\":7}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertTrue(response.responseBody, response.responseBody.contains("\"cog\""));
        client.assertNoMoreExceptions();
    }

    @Test
    public void queryInsideTransactionReadsCommittedRows() throws Exception {
        databaseClient.update(null, "INSERT INTO widget (id, name) VALUES (?, ?)", 8L, "gear")
                .toCompletionStage().toCompletableFuture().join();

        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/find-tx").body("{\"id\":8}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertTrue(response.responseBody, response.responseBody.contains("\"gear\""));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedUpdateInTransactionInsertsRow() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/named").body("{\"id\":3,\"name\":\"piston\"}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertEquals(List.of("piston"), selectWidgetNames(3L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void batchInsertInTransactionInsertsAllRows() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/batch").body(
                        "{\"widgets\":[{\"id\":10,\"name\":\"a\"},{\"id\":11,\"name\":\"b\"},{\"id\":12,\"name\":\"c\"}]}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertEquals(List.of("a"), selectWidgetNames(10L));
        assertEquals(List.of("b"), selectWidgetNames(11L));
        assertEquals(List.of("c"), selectWidgetNames(12L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void batchInsertNamedInTransactionInsertsAllRows() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/batch-named").body(
                        "{\"widgets\":[{\"id\":20,\"name\":\"x\"},{\"id\":21,\"name\":\"y\"}]}"));

        assertEquals(response.responseBody, 200, response.statusCode);
        assertEquals(List.of("x"), selectWidgetNames(20L));
        assertEquals(List.of("y"), selectWidgetNames(21L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void atomicityAcrossMultipleStatements() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/two-then-fail").body(
                        "{\"widgets\":[{\"id\":30,\"name\":\"first\"},{\"id\":31,\"name\":\"second\"}]}"));

        assertEquals(500, response.statusCode);
        assertEquals(List.of(), selectWidgetNames(30L));
        assertEquals(List.of(), selectWidgetNames(31L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void constraintViolationRollsBackEarlierInsertsInSameTx() throws Exception {
        final TestHttpResponse response = client.post(
                StubRequest.request("/widgets/duplicate-pk").body(
                        "{\"widgets\":[{\"id\":40,\"name\":\"keep\"},{\"id\":40,\"name\":\"clash\"}]}"));

        assertEquals(500, response.statusCode);
        assertEquals(List.of(), selectWidgetNames(40L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void connectionsAreReleasedAfterRepeatedRollback() throws Exception {
        for (int i = 0; i < 50; i++) {
            final TestHttpResponse failed = client.post(
                    StubRequest.request("/users/rollback").body("{\"id\":" + i + ",\"name\":\"x\"}"));
            assertEquals(500, failed.statusCode);
        }
        assertEquals(List.of(), selectUserNames(0L));

        final TestHttpResponse ok = client.post(
                StubRequest.request("/users").body("{\"id\":500,\"name\":\"survivor\"}"));
        assertEquals(ok.responseBody, 200, ok.statusCode);
        assertEquals(List.of("survivor"), selectUserNames(500L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedParamCoexistsWithColonInsideStringLiteral() throws Exception {
        runNamedUpdate(
                "INSERT INTO widget (id, name) VALUES (:id, ':not_a_param')",
                Map.of("id", 60L));
        assertEquals(List.of(":not_a_param"), selectWidgetNames(60L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedParamIgnoresColonInsideLineComment() throws Exception {
        runNamedUpdate(
                "INSERT INTO widget (id, name) VALUES (:id, :name) -- :ignored extra :stuff\n",
                Map.of("id", 61L, "name", "linecomment"));
        assertEquals(List.of("linecomment"), selectWidgetNames(61L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedParamIgnoresColonInsideHashComment() throws Exception {
        runNamedUpdate(
                "INSERT INTO widget (id, name) VALUES (:id, :name) # :ignored\n",
                Map.of("id", 62L, "name", "hashcomment"));
        assertEquals(List.of("hashcomment"), selectWidgetNames(62L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedParamIgnoresColonInsideBlockComment() throws Exception {
        runNamedUpdate(
                "/* :ignored :things */ INSERT INTO widget (id, name) VALUES (:id, :name)",
                Map.of("id", 63L, "name", "blockcomment"));
        assertEquals(List.of("blockcomment"), selectWidgetNames(63L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void namedParamIgnoresColonInsideDoubledQuoteEscape() throws Exception {
        runNamedUpdate(
                "INSERT INTO widget (id, name) VALUES (:id, 'it''s :hidden')",
                Map.of("id", 64L));
        assertEquals(List.of("it's :hidden"), selectWidgetNames(64L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void doubleColonNotTreatedAsNamedParam() throws Exception {
        runNamedUpdate(
                "INSERT INTO widget (id, name) VALUES (:id, ':1abc')",
                Map.of("id", 65L));
        assertEquals(List.of(":1abc"), selectWidgetNames(65L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void sameNamedParamReferencedTwiceBindsTwice() throws Exception {
        databaseClient.update(null, "INSERT INTO widget (id, name) VALUES (?, ?)", 70L, "first")
                .toCompletionStage().toCompletableFuture().join();
        databaseClient.update(null, "INSERT INTO widget (id, name) VALUES (?, ?)", 71L, "first")
                .toCompletionStage().toCompletableFuture().join();
        databaseClient.update(null, "INSERT INTO widget (id, name) VALUES (?, ?)", 72L, "other")
                .toCompletionStage().toCompletableFuture().join();

        runNamedUpdate(
                "DELETE FROM widget WHERE id = :id OR name = :name AND id <> :id",
                Map.of("id", 70L, "name", "first"));

        assertEquals(List.of(), selectWidgetNames(70L));
        assertEquals(List.of(), selectWidgetNames(71L));
        assertEquals(List.of("other"), selectWidgetNames(72L));
        client.assertNoMoreExceptions();
    }

    @Test
    public void missingNamedParamRaisesError() throws Exception {
        final TestHttpResponse response = client.post(StubRequest.request("/sql/named-update").body(
                "{\"sql\":\"INSERT INTO widget (id, name) VALUES (:id, :bogus)\"," +
                        "\"params\":{\"id\":80}}"));
        assertEquals(500, response.statusCode);
        assertEquals(List.of(), selectWidgetNames(80L));
    }

    private void runNamedUpdate(final String sql, final Map<String, Object> params) {
        final StringBuilder body = new StringBuilder("{\"sql\":");
        appendJsonString(body, sql);
        body.append(",\"params\":{");
        boolean first = true;
        for (final Map.Entry<String, Object> e : params.entrySet()) {
            if (!first) body.append(',');
            first = false;
            body.append('"').append(e.getKey()).append("\":");
            final Object v = e.getValue();
            if (v instanceof String s) {
                appendJsonString(body, s);
            } else {
                body.append(v);
            }
        }
        body.append("}}");
        final TestHttpResponse response = client.post(StubRequest.request("/sql/named-update").body(body.toString()));
        assertEquals(response.responseBody, 200, response.statusCode);
    }

    private static void appendJsonString(final StringBuilder out, final String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default: out.append(c);
            }
        }
        out.append('"');
    }

    private List<String> selectUserNames(final long id) {
        return databaseClient.query(null, "SELECT name FROM users WHERE id = ?",
                row -> row.getString("name"), id)
                .toCompletionStage().toCompletableFuture().join();
    }

    private List<String> selectWidgetNames(final long id) {
        return databaseClient.query(null, "SELECT name FROM widget WHERE id = ?",
                row -> row.getString("name"), id)
                .toCompletionStage().toCompletableFuture().join();
    }

    public static final class CreateUserRequest {
        public long id;
        public String name;
    }

    public static final class CreateWidgetRequest {
        public long id;
        public String name;
    }

    public static final class CreateWidgetsRequest {
        public List<CreateWidgetRequest> widgets;
    }

    public static final class FindWidgetRequest {
        public long id;
    }

    public static final class NamedSqlRequest {
        public String sql;
        public Map<String, Object> params;
    }

    public static final class FindWidgetResponse {
        public List<String> names;

        public FindWidgetResponse(final List<String> names) {
            this.names = names;
        }
    }

    private static class CreateUserTransactionallyHandler implements JsonHandler<CreateUserRequest, CreateUserRequest, Object> {
        @Override
        public LuxisPipeline<CreateUserRequest> handle(final HttpStream<CreateUserRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO users (id, name) VALUES (?, ?)",
                                    ctx.in().id, ctx.in().name))
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class RollbackUserHandler implements JsonHandler<CreateUserRequest, CreateUserRequest, Object> {
        @Override
        public LuxisPipeline<CreateUserRequest> handle(final HttpStream<CreateUserRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO users (id, name) VALUES (?, ?)",
                                    ctx.in().id, ctx.in().name))
                            .<CreateUserRequest>flatMap(ctx -> HttpResult.error(ErrorStatusCode.INTERNAL_SERVER_ERROR,
                                    new ErrorMessageResponse("forced rollback")))
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class CreateWidgetNonTxHandler implements JsonHandler<CreateWidgetRequest, CreateWidgetRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetRequest> handle(final HttpStream<CreateWidgetRequest, Object> stream) {
            return stream
                    .asyncPeek(ctx -> ctx.db().update(
                            "INSERT INTO widget (id, name) VALUES (?, ?)",
                            ctx.in().id, ctx.in().name))
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class FindWidgetNonTxHandler implements JsonHandler<FindWidgetRequest, FindWidgetResponse, Object> {
        @Override
        public LuxisPipeline<FindWidgetResponse> handle(final HttpStream<FindWidgetRequest, Object> stream) {
            return stream
                    .asyncMap(ctx -> ctx.<Row, Long>db().query(
                            "SELECT name FROM widget WHERE id = ?",
                            row -> row.getString("name"), ctx.in().id))
                    .complete(ctx -> HttpResult.success(new FindWidgetResponse(ctx.in())));
        }
    }

    private static class FindWidgetInTxHandler implements JsonHandler<FindWidgetRequest, FindWidgetResponse, Object> {
        @Override
        public LuxisPipeline<FindWidgetResponse> handle(final HttpStream<FindWidgetRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncMap(ctx -> ctx.<Row, Long>db().query(
                                    "SELECT name FROM widget WHERE id = ?",
                                    row -> row.getString("name"), ctx.in().id))
                            .commit())
                    .complete(ctx -> HttpResult.success(new FindWidgetResponse(ctx.in())));
        }
    }

    private static class CreateWidgetNamedHandler implements JsonHandler<CreateWidgetRequest, CreateWidgetRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetRequest> handle(final HttpStream<CreateWidgetRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                final Map<String, Object> params = new HashMap<>();
                                params.put("id", ctx.in().id);
                                params.put("name", ctx.in().name);
                                return ctx.db().update(
                                        "INSERT INTO widget (id, name) VALUES (:id, :name)", params);
                            })
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class BatchInsertWidgetsHandler implements JsonHandler<CreateWidgetsRequest, CreateWidgetsRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetsRequest> handle(final HttpStream<CreateWidgetsRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                final List<Object[]> rows = ctx.in().widgets.stream()
                                        .map(w -> new Object[]{w.id, w.name})
                                        .toList();
                                return ctx.db().updateBatch(
                                        "INSERT INTO widget (id, name) VALUES (?, ?)", rows);
                            })
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class RawNamedUpdateHandler implements JsonHandler<NamedSqlRequest, NamedSqlRequest, Object> {
        @Override
        public LuxisPipeline<NamedSqlRequest> handle(final HttpStream<NamedSqlRequest, Object> stream) {
            return stream
                    .asyncPeek(ctx -> ctx.db().update(ctx.in().sql, ctx.in().params))
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class TwoInsertsThenFailHandler implements JsonHandler<CreateWidgetsRequest, CreateWidgetsRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetsRequest> handle(final HttpStream<CreateWidgetsRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO widget (id, name) VALUES (?, ?)",
                                    ctx.in().widgets.get(0).id, ctx.in().widgets.get(0).name))
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO widget (id, name) VALUES (?, ?)",
                                    ctx.in().widgets.get(1).id, ctx.in().widgets.get(1).name))
                            .<CreateWidgetsRequest>flatMap(ctx -> HttpResult.error(ErrorStatusCode.INTERNAL_SERVER_ERROR,
                                    new ErrorMessageResponse("forced rollback")))
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class DuplicatePkInTxHandler implements JsonHandler<CreateWidgetsRequest, CreateWidgetsRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetsRequest> handle(final HttpStream<CreateWidgetsRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO widget (id, name) VALUES (?, ?)",
                                    ctx.in().widgets.get(0).id, ctx.in().widgets.get(0).name))
                            .asyncPeek(ctx -> ctx.db().update(
                                    "INSERT INTO widget (id, name) VALUES (?, ?)",
                                    ctx.in().widgets.get(1).id, ctx.in().widgets.get(1).name))
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }

    private static class BatchInsertWidgetsNamedHandler implements JsonHandler<CreateWidgetsRequest, CreateWidgetsRequest, Object> {
        @Override
        public LuxisPipeline<CreateWidgetsRequest> handle(final HttpStream<CreateWidgetsRequest, Object> stream) {
            return stream
                    .inTransaction(tx -> tx
                            .asyncPeek(ctx -> {
                                final List<Map<String, Object>> rows = ctx.in().widgets.stream()
                                        .map(w -> {
                                            final Map<String, Object> m = new HashMap<>();
                                            m.put("id", w.id);
                                            m.put("name", w.name);
                                            return m;
                                        })
                                        .toList();
                                return ctx.db().updateBatchNamed(
                                        "INSERT INTO widget (id, name) VALUES (:id, :name)", rows);
                            })
                            .commit())
                    .complete(ctx -> HttpResult.success(ctx.in()));
        }
    }
}
