package io.kiw.luxis.mysql;

import io.kiw.luxis.web.db.BatchUpdateResult;
import io.kiw.luxis.web.db.DatabaseClient;
import io.kiw.luxis.web.db.UpdateResult;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.internal.ArrayTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MysqlDatabaseClient implements DatabaseClient<MysqlTransaction, Row, Long> {

    private final Pool pool;

    public MysqlDatabaseClient(final Pool pool) {
        this.pool = pool;
    }

    @Override
    public Future<MysqlTransaction> begin() {
        return pool.getConnection().compose(connection ->
                connection.begin()
                        .map(transaction -> new MysqlTransaction(connection, transaction))
                        .onFailure(err -> connection.close()));
    }

    @Override
    public Future<Void> commit(final MysqlTransaction tx) {
        return tx.transaction().commit().eventually(() -> tx.connection().close());
    }

    @Override
    public Future<Void> rollback(final MysqlTransaction tx) {
        return tx.transaction().rollback().eventually(() -> tx.connection().close());
    }

    @Override
    public <T> Future<List<T>> query(final MysqlTransaction tx, final String sql, final Function<Row, T> rowMapper, final Object... params) {


        return resolveClient(tx).preparedQuery(sql)
                .mapping(rowMapper)
                .execute(new ArrayTuple(Arrays.stream(params).toList()))
                .map(ts -> ts.stream().toList());
    }

    private SqlClient resolveClient(final MysqlTransaction tx) {
        if (tx == null) {
            return pool;
        } else {
            return tx.connection();
        }
    }

    @Override
    public <T> Future<List<T>> query(final MysqlTransaction tx, final String sql, final Function<Row, T> rowMapper, final Map<String, Object> params) {
        final NamedSql named = translate(sql);
        return resolveClient(tx).preparedQuery(named.sql)
                .mapping(rowMapper)
                .execute(toTuple(named.names, params))
                .map(ts -> ts.stream().toList());
    }

    @Override
    public Future<UpdateResult<Long>> update(final MysqlTransaction tx, final String sql, final Object... params) {
        return resolveClient(tx).preparedQuery(sql).execute(new ArrayTuple(Arrays.stream(params).toList())).map(rs -> new UpdateResult<>(rs.rowCount(), rs.property(MySQLClient.LAST_INSERTED_ID)));
    }

    @Override
    public Future<UpdateResult<Long>> update(final MysqlTransaction tx, final String sql, final Map<String, Object> params) {
        final NamedSql named = translate(sql);
        return resolveClient(tx).preparedQuery(named.sql)
                .execute(toTuple(named.names, params))
                .map(rs -> new UpdateResult<>(rs.rowCount(), rs.property(MySQLClient.LAST_INSERTED_ID)));
    }

    @Override
    public Future<BatchUpdateResult<Long>> updateBatch(final MysqlTransaction tx, final String sql, final List<Object[]> rows) {
        return resolveClient(tx).preparedQuery(sql)
                .executeBatch(rows.stream().map(arr -> Tuple.from(Arrays.asList(arr))).toList())
                .map(s -> {
                    final Long lastId = s.property(MySQLClient.LAST_INSERTED_ID);
                    return new BatchUpdateResult<>(new int[] {s.rowCount()}, lastId == null ? List.of() : List.of(lastId));
                });
    }

    @Override
    public Future<BatchUpdateResult<Long>> updateBatchNamed(final MysqlTransaction tx, final String sql, final List<Map<String, Object>> rows) {
        final NamedSql named = translate(sql);
        final List<Tuple> tuples = rows.stream().map(r -> toTuple(named.names, r)).toList();
        return resolveClient(tx).preparedQuery(named.sql)
                .executeBatch(tuples)
                .map(s -> {
                    final Long lastId = s.property(MySQLClient.LAST_INSERTED_ID);
                    return new BatchUpdateResult<>(new int[] {s.rowCount()}, lastId == null ? List.of() : List.of(lastId));
                });
    }

    private static final class NamedSql {
        final String sql;
        final List<String> names;

        NamedSql(final String sql, final List<String> names) {
            this.sql = sql;
            this.names = names;
        }
    }

    private static NamedSql translate(final String sql) {
        final StringBuilder out = new StringBuilder(sql.length());
        final List<String> names = new ArrayList<>();
        final int n = sql.length();
        int i = 0;
        while (i < n) {
            final char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                final char quote = c;
                out.append(c);
                i++;
                while (i < n) {
                    final char d = sql.charAt(i);
                    if (d == '\\' && i + 1 < n) {
                        out.append(d).append(sql.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    out.append(d);
                    i++;
                    if (d == quote) {
                        if (i < n && sql.charAt(i) == quote) {
                            out.append(quote);
                            i++;
                            continue;
                        }
                        break;
                    }
                }
                continue;
            }
            if ((c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') || c == '#') {
                while (i < n && sql.charAt(i) != '\n') {
                    out.append(sql.charAt(i));
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                out.append("/*");
                i += 2;
                while (i < n) {
                    if (sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/') {
                        out.append("*/");
                        i += 2;
                        break;
                    }
                    out.append(sql.charAt(i));
                    i++;
                }
                continue;
            }
            if (c == ':' && i + 1 < n && isNameStart(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < n && isNamePart(sql.charAt(j))) {
                    j++;
                }
                names.add(sql.substring(i + 1, j));
                out.append('?');
                i = j;
                continue;
            }
            out.append(c);
            i++;
        }
        return new NamedSql(out.toString(), names);
    }

    private static boolean isNameStart(final char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isNamePart(final char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static Tuple toTuple(final List<String> names, final Map<String, Object> params) {
        final Tuple tuple = Tuple.tuple();
        for (final String name : names) {
            if (!params.containsKey(name)) {
                throw new IllegalArgumentException("Missing parameter: " + name);
            }
            tuple.addValue(params.get(name));
        }
        return tuple;
    }
}
