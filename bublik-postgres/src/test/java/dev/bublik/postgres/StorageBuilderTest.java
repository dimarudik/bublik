package dev.bublik.postgres;

import dev.bublik.core.model.DummyTable;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

public class StorageBuilderTest {

    @Test
    void testBuilderWithExplicitThreadCount() throws Exception {
        DataSource mockDataSource = Mockito.mock(DataSource.class);
        int expectedThreadCount = 7;

        Storage storage = new PostgresStorage.Builder(mockDataSource)
                .threadCount(expectedThreadCount)
                .outboxTable(new DummyTable("public", "bublik"))
                .build();


        assertEquals(expectedThreadCount, storage.getThreadCount(),
                "Поле threadCount должно быть строго равно переданному значению");

        Field isManagedPoolField = Storage.class.getDeclaredField("isManaged");
        isManagedPoolField.setAccessible(true);
        boolean isManagedPool = (boolean) isManagedPoolField.get(storage);

        assertFalse(isManagedPool,
                "Флаг isManagedPool должен быть false, так как DataSource передан снаружи");

        Field dataSourceField = JDBCStorage.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        DataSource actualDataSource = (DataSource) dataSourceField.get(storage);

        assertEquals(mockDataSource, actualDataSource,
                "DataSource внутри хранилища должен совпадать с переданным в конструктор");
    }
}
