package io.kiw.luxis.mysql;

import io.kiw.luxis.web.TransactionManager;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;

public final class MySqlTransactionManager implements TransactionManager<MysqlTransaction> {

    private final Pool pool;

    public MySqlTransactionManager(final Pool pool) {
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
}
