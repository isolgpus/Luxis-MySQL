package io.kiw.luxis.mysql;

import io.kiw.luxis.result.Result;
import io.kiw.luxis.web.http.client.LuxisAsync;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class LuxisMysqlClient {

    private final SqlClient client;

    public LuxisMysqlClient(final SqlClient client) {
        this.client = client;
    }



    public <T, ERR> LuxisAsync<List<T>, ERR> query(
            final String sql, final Tuple params, final Function<Row, T> mapper) {
        return toLuxisAsync(client.preparedQuery(sql)
                .mapping(mapper)
                .execute(params)
                .map(LuxisMysqlClient::toList));
    }

    public <T, ERR> LuxisAsync<Integer, ERR> updateBatch(
            final String sql, final List<Tuple> batch) {
        return toLuxisAsync(client.preparedQuery(sql)
                .executeBatch(batch)
                .map(RowSet::rowCount));
    }

    public <ERR> LuxisAsync<Integer, ERR> update(final String sql, final Tuple params) {
        return toLuxisAsync(client.preparedQuery(sql).execute(params).map(RowSet::rowCount));
    }

    private static <T> List<T> toList(final RowSet<T> rowSet) {
        final List<T> list = new ArrayList<>();
        rowSet.forEach(list::add);
        return list;
    }

    private static <T, ERR> LuxisAsync<T, ERR> toLuxisAsync(final Future<T> future) {
        final CompletableFuture<Result<ERR, T>> cf = new CompletableFuture<>();
        future.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(Result.success(ar.result()));
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return new LuxisAsync<>(cf);
    }
}
