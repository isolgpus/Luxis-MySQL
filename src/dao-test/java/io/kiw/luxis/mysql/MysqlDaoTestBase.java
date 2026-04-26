package io.kiw.luxis.mysql;

import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLBuilder;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class MysqlDaoTestBase {

    private static final MySQLContainer MYSQL = new MySQLContainer<>("mysql:9.7.0")
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("mysql-container")))
            .withConfigurationOverride("mysql-empty-conf");



    private static Vertx vertx;
    private static Pool pool;

    @BeforeClass
    public static void startPool() {
        MYSQL.start();
        vertx = Vertx.vertx();
        final MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setHost(MYSQL.getHost())
                .setPort(MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT))
                .setDatabase(MYSQL.getDatabaseName())
                .setUser(MYSQL.getUsername())
                .setPassword(MYSQL.getPassword());
        pool = MySQLBuilder.pool()
                .with(new PoolOptions().setMaxSize(4))
                .connectingTo(connectOptions)
                .using(vertx)
                .build();
    }

    @AfterClass
    public static void stopPool() throws Exception {
        if (pool != null) {
            pool.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        MYSQL.stop();
    }

    protected static Pool pool() {
        return pool;
    }
}
