package dev.bublik.starter.autoconfigure;

import dev.bublik.starter.properties.BublikProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BublikStarterPostgresMigrationTest {
    static final JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres"));

    @BeforeAll
    static void beforeAll() throws Exception {
        postgres.start();

        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE public.source_users (id INT PRIMARY KEY, user_name VARCHAR(100))");
            stmt.execute("CREATE TABLE public.target_users (id INT PRIMARY KEY, user_name VARCHAR(100))");

            stmt.execute("INSERT INTO public.source_users (id, user_name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        }
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @Test
    void shouldAutomaticallyExecuteMigrationOnContextStartup() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(BublikAutoConfiguration.class))
                .withPropertyValues(
                        "bublik.enabled=true",
                        "bublik.trigger-mode=ON_START",
                        "bublik.thread-count=2",

                        "bublik.from.url=" + postgres.getJdbcUrl(),
                        "bublik.from.user=" + postgres.getUsername(),
                        "bublik.from.password=" + postgres.getPassword(),

                        "bublik.to.url=" + postgres.getJdbcUrl(),
                        "bublik.to.user=" + postgres.getUsername(),
                        "bublik.to.password=" + postgres.getPassword(),

                        "bublik.pipelines[0].from-schema=public",
                        "bublik.pipelines[0].from-table=source_users",
                        "bublik.pipelines[0].to-schema=public",
                        "bublik.pipelines[0].to-table=target_users"
                );

        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(BublikAutoConfiguration.class);
            assertThat(context).hasSingleBean(BublikProperties.class);
            assertThat(context).hasBean("automatedMigrationRunner");

            ApplicationRunner runner = context.getBean(
                    "automatedMigrationRunner", org.springframework.boot.ApplicationRunner.class);

            System.out.println("[TEST] Manually invoking automatedMigrationRunner inside simulated context...");
            runner.run(null);

            System.out.println("[TEST] Spring Boot Context verified. Checking СУБД target table results...");

            try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*), MIN(user_name) FROM public.target_users")) {

                assertTrue(rs.next(), "Должна вернуться результирующая строка");
                int count = rs.getInt(1);
                String firstUser = rs.getString(2);

                assertEquals(3, count, "Стартер должен был автоматически перенести ровно 3 записи при старте контекста");
                assertEquals("Alice", firstUser, "Данные внутри перенесенных строк должны полностью совпадать");

                System.out.println("[TEST] SUCCESS! Automated end-to-end starter migration verified.");
            }
        });
    }
}
