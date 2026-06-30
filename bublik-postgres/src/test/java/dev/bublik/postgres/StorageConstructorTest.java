package dev.bublik.postgres;

import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.postgres.storage.PostgresStorage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

public class StorageConstructorTest {

    @Test
    void testConstructorWithExplicitThreadCount() throws Exception {
        DataSource mockDataSource = Mockito.mock(DataSource.class);
        int expectedThreadCount = 7;

        Storage storage = new PostgresStorage(mockDataSource, expectedThreadCount, new PseudoTable("public", "bublik"));

        assertNotNull(storage.getConnectionProperty(),
                "ConnectionProperty не должен быть null, иначе будет NullPointerException");

        assertEquals(expectedThreadCount, storage.getThreadCount(),
                "Поле threadCount должно быть строго равно переданному значению");

        Field isManagedPoolField = JDBCStorage.class.getDeclaredField("isManagedPool");
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
