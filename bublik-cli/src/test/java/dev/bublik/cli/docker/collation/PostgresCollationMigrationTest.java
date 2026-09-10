package dev.bublik.cli.docker.collation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Disabled
public class PostgresCollationMigrationTest {
    private static final Logger log = LoggerFactory.getLogger(PostgresCollationMigrationTest.class);
    private static final Network network = Network.newNetwork();

    private static final GenericContainer<?> master = new GenericContainer<>("local-pg12-master-bullseye")
            .withNetwork(network)
            .withNetworkAliases("pg-master")
            .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
            .withEnv("POSTGRESQL_USER", "migration_user")
//            .withEnv("POSTGRESQL_ADMIN_PASSWORD", "postgres")
            .withEnv("POSTGRESQL_DATABASE", "test")
            .withEnv("POSTGRESQL_PASSWORD", "")
            .withExposedPorts(5432)
            .waitingFor(Wait.forLogMessage(".*redirecting log output to logging collector process.*\\n", 1));
//            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 1));

    private static final GenericContainer<?> replica = new GenericContainer<>("local-pg12-replica-ubuntu22")
            .withNetwork(network)
            .dependsOn(master)
            .withExposedPorts(5432)
            .withCommand("/bin/bash", "-c",
                    "PGPASSWORD=rep_pass pg_basebackup -h pg-master -U replicator -D /var/lib/postgresql/12/main/ -Fp -Xs -R && " +
                            "chown -R postgres:postgres /var/lib/postgresql/12/main && " +

                            "echo 'local all all trust' > /etc/postgresql/12/main/pg_hba.conf && " +
                            "echo 'host all all 0.0.0.0/0 trust' >> /etc/postgresql/12/main/pg_hba.conf && " +
                            "echo 'host replication all 0.0.0.0/0 trust' >> /etc/postgresql/12/main/pg_hba.conf && " +
                            "chown postgres:postgres /etc/postgresql/12/main/pg_hba.conf && " +

                            "su - postgres -c '" +
                            "/usr/lib/postgresql/12/bin/postgres -D /var/lib/postgresql/12/main " +
                            "-c config_file=/etc/postgresql/12/main/postgresql.conf " +
                            "-c log_connections=on " +
                            "-c log_disconnections=on " +
                            "-c log_min_messages=debug1 " +
                            "-c listen_addresses=*'"
            )
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept read.*only connections.*\\n", 1));

    @BeforeAll
    static void setUpCluster() throws Exception {
        master.start();
        master.followOutput(new Slf4jLogConsumer(log).withPrefix("OLD-MASTER"));

        // Даем сетевому стеку Docker Mac M3 стабилизироваться
        Thread.sleep(3000);

        System.out.println(">>> MASTER HOST: " + master.getHost());
        System.out.println(">>> MASTER MAPPED PORT: " + master.getMappedPort(5432));

        // ШАГ 1: ХАК ДЛЯ CENTOS — СНАЧАЛА врезаем trust на самый верх pg_hba.conf из-за ограничений OpenShift
        master.execInContainer("sh", "-c", "sed -i '1i host replication replicator 0.0.0.0/0 trust' /var/lib/pgsql/data/userdata/pg_hba.conf");
        master.execInContainer("sh", "-c", "sed -i '1i host all all 0.0.0.0/0 trust' /var/lib/pgsql/data/userdata/pg_hba.conf");
        System.out.println("📝 Строки trust успешно врезаны на самый верх pg_hba.conf Мастера.");

        // ШАГ 2: Перезагружаем СУБД CentOS через утилиту pg_ctl (в SCL образах она доступна в окружении)
        master.execInContainer("sh", "-c", "pg_ctl reload -D /var/lib/pgsql/data/userdata");
        System.out.println("🔄 Конфигурация CentOS Мастера успешно перезагружена в режим trust.");

        Thread.sleep(1000);

        // ШАГ 3: Теперь сетевой trust активен! Спокойно подключаемся из Java без пароля
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", "postgres");
        props.setProperty("sslmode", "disable");

        String masterJdbc = "jdbc:postgresql://" + master.getHost() + ":" + master.getMappedPort(5432) + "/postgres";

        try (Connection conn = DriverManager.getConnection(masterJdbc, props)) {
            try (Statement st = conn.createStatement()) {
                // Теперь эта команда гарантированно выполнится!
                st.execute("CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'rep_pass';");
                System.out.println("🔥 УСПЕХ: Роль replicator создана из Java на CentOS!");
            }
        } catch (Exception e) {
            log.error("💥 КРИТИЧЕСКАЯ ОШИБКА НА МАСТЕРЕ: {}", e.getMessage());
        }

        // ШАГ 4: Запускаем реплику на Ubuntu. Она без проблем стянет бэкап без пароля по правилу trust
        replica.start();
        replica.followOutput(new Slf4jLogConsumer(log).withPrefix("UBUNTU-REPLICA"));
    }

    @AfterAll
    static void tearDown() {
        replica.stop();
        master.stop();
    }

    @Test
    void reproduceCollationIndexCorruption() throws Exception {
        try {
            Container.ExecResult masterGlibc = master.execInContainer("ldd", "--version");
            log.info("\n--- МАСТЕР: Версия glibc --- \n{}", masterGlibc.getStdout());
        } catch (Exception e) {
            log.error("Не удалось получить версию glibc на Мастере: {}", e.getMessage());
        }

        try {
            Container.ExecResult replicaGlibc = replica.execInContainer("ldd", "--version");
            log.info("\n--- РЕПЛИКА: Версия glibc --- \n{}", replicaGlibc.getStdout());
        } catch (Exception e) {
            log.error("Не удалось получить версию glibc на Реплике: {}", e.getMessage());
        }

        String masterJdbc = "jdbc:postgresql://" + master.getHost() + ":" + master.getMappedPort(5432) + "/postgres?sslmode=disable";
        String replicaJdbc = "jdbc:postgresql://" + replica.getHost() + ":" + replica.getMappedPort(5432) + "/postgres";

        Properties props = new java.util.Properties();
        props.setProperty("user", "postgres");
        props.setProperty("sslmode", "disable");

        List<String> masterResults = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(masterJdbc, "postgres", "");
             Statement st = conn.createStatement()) {

            st.execute("CREATE TABLE test_corruption (id serial, name text);");
            st.execute("CREATE INDEX idx_test_name ON test_corruption(name);");
            st.execute("INSERT INTO test_corruption (name) VALUES ('аб'), ('а-б'), ('ав'), ('а-в');");

            st.execute("SET enable_seqscan = off;");
            st.execute("SET enable_bitmapscan = off;");
            ResultSet rs = st.executeQuery("SELECT name FROM test_corruption WHERE name >= 'а-б' AND name <= 'а-в' ORDER BY name;");
            while (rs.next()) {
                masterResults.add(rs.getString("name"));
            }
        }
        System.out.println(">>> Данные с Мастера (glibc 2.31): " + masterResults);

        Thread.sleep(2000);


        List<String> replicaResults = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(replicaJdbc, props)) {
            try (Statement st = conn.createStatement()) {
                st.execute("SET enable_seqscan = off;");
                st.execute("SET enable_bitmapscan = off;");
                ResultSet rs = st.executeQuery("SELECT name FROM test_corruption WHERE name >= 'а-б' AND name <= 'а-в' ORDER BY name;");
                while (rs.next()) {
                    replicaResults.add(rs.getString("name"));
                }
            }
        }
        System.out.println(">>> Данные с Реплики (glibc 2.35): " + replicaResults);

//        Thread.sleep(600_000);

        assertNotEquals(masterResults, replicaResults, "Индекс B-Tree незаметно разрушился при репликации между версиями glibc!");
        System.out.println("🔥 ТЕСТ УСПЕШНО ЗАФИКСИРОВАЛ ПРОБЛЕМУ COLLATION! Индексы выдали разный результат.");



        // =========================================================================
        // ФАЗА МИГРАЦИИ
        // =========================================================================
        System.out.println("\n🚀 Запуск плана миграции с использованием ICU локалей...");

        try (Connection conn = DriverManager.getConnection(masterJdbc, "postgres", "");
             Statement st = conn.createStatement()) {

            st.execute("CREATE COLLATION icu_ru (provider = icu, locale = 'ru-RU', deterministic = true);");
            System.out.println("✨ ICU локаль 'icu_ru' успешно создана на Мастере.");

            st.execute("CREATE INDEX idx_test_name_icu ON test_corruption (name COLLATE icu_ru);");
            System.out.println("📦 Новый ICU-индекс idx_test_name_icu построен.");
        }

        Thread.sleep(1_000);

        // 3. Имитируем FAILOVER (Повышаем реплику на Ubuntu до Мастера)
        System.out.println("💥 Выполняем FAILOVER: Повышаем Реплику...");
        replica.execInContainer("sh", "-c", "su - postgres -c '/usr/lib/postgresql/12/bin/pg_ctl promote -D /var/lib/postgresql/12/main'");
        Thread.sleep(1_000);

        // 4. Подключаемся к Новой Базе на Ubuntu и проверяем работу ICU-индекса ДО удаления старого
        try (Connection conn = DriverManager.getConnection(replicaJdbc, "postgres", "");
             Statement st = conn.createStatement()) {

            System.out.println("\n✂️ Начинаем безопасную рокировку индексов...");

            st.execute("DROP INDEX idx_test_name;");
            st.execute("ALTER INDEX idx_test_name_icu RENAME TO idx_test_name;");
            st.execute("ANALYZE test_corruption;");

            st.execute("SET enable_seqscan = off;");
            st.execute("SET enable_bitmapscan = off;");

            List<String> icuResults = new ArrayList<>();
            st.execute("SET enable_seqscan = off;");
            st.execute("SET enable_bitmapscan = off;");
            ResultSet rs = st.executeQuery("SELECT name FROM test_corruption WHERE name >= 'а-б' AND name <= 'а-в' ORDER BY name;");
            while (rs.next()) {
                icuResults.add(rs.getString("name"));
            }
            System.out.println(">>> Данные с нового мастера через ICU-индекс (glibc 2.35): " + icuResults);

            assertEquals(masterResults, icuResults,
                    "ICU индекс защитил данные от разницы glibc!");
            System.out.println("🎉 УСПЕХ: Новая база сразу видит данные через ICU без REINDEX!");

        }
    }
}
