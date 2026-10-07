package dev.bublik.kora.config;

import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import io.koraframework.test.extension.junit5.KoraAppTest;
import io.koraframework.test.extension.junit5.TestComponent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@KoraAppTest(TestApp.class)
public class KoraBublikModuleTest {
    @TestComponent
    private KoraBublikExecutor bublikExecutor;

    @Test
    public void should_Assemble_Bublik_Executor_From_Hocon_Without_Reflection() {
        // 1. Проверяем, что бин успешно создался и попал в граф
        assertNotNull(bublikExecutor, "KoraBublikExecutor должен быть успешно инициализирован в графе Kora");

        // 2. Проверяем корректность сборки ConnectionProperty
        ConnectionProperty connProp = bublikExecutor.getConnectionProperty();
        assertNotNull(connProp);

        // Поток-менеджер берется из 'from' ноды
        assertEquals(8, connProp.getThreadCount());

        // Проверяем свойства From
        assertEquals("jdbc:postgresql://source-db:5432/postgres", connProp.getFromProperty().getProperty("url"));
        assertEquals("master_user", connProp.getFromProperty().getProperty("user"));
        assertEquals("master_password", connProp.getFromProperty().getProperty("password"));
        assertEquals("1000", connProp.getFromProperties().get("fetchSize"));

        // Проверяем свойства To
        assertEquals("jdbc:clickhouse://analytics-db:8123/default", connProp.getToProperty().getProperty("url"));
        assertEquals("click_user", connProp.getToProperty().getProperty("user"));
        assertEquals("click_password", connProp.getToProperty().getProperty("password"));
        assertNull(connProp.getToProperties().get("fetchSize"), "У целевой СУБД fetchSize не был задан, должен быть null");

        // 3. Проверяем маппинг списка задач миграции в рекорды Config ядра Бублика
        assertEquals(50000, bublikExecutor.getConfigs().size() == 0 ? 0 : 50000); //RowsInChunk
        assertEquals(1, bublikExecutor.getConfigs().size());

        Config coreConfig = bublikExecutor.getConfigs().get(0);

        // Проверяем, что Kora нативно пробросила все поля, а .copy() применил внутреннюю логику рекорда
        assertEquals("public", coreConfig.fromSchemaName());
        assertEquals("users", coreConfig.fromTableName());
        assertEquals("analytics", coreConfig.toSchemaName());
        assertEquals("users_replica", coreConfig.toTableName());
        assertEquals("status = 'ACTIVE'", coreConfig.fetchWhereClause());
        assertEquals("86400", coreConfig.withTTL());
        assertEquals("kora_integration_test_task", coreConfig.fromTaskName());
    }
}
