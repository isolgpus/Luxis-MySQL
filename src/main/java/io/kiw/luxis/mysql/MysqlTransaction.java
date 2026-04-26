package io.kiw.luxis.mysql;

import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;

public final class MysqlTransaction {

    private final SqlConnection connection;
    private final Transaction transaction;

    MysqlTransaction(final SqlConnection connection, final Transaction transaction) {
        this.connection = connection;
        this.transaction = transaction;
    }

    public SqlConnection connection() {
        return connection;
    }

    public Transaction transaction() {
        return transaction;
    }
}
