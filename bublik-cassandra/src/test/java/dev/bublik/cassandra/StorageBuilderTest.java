package dev.bublik.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import dev.bublik.cassandra.storage.CassandraStorage;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.storage.Storage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StorageBuilderTest {
    @Test
    void testBuilder() throws Exception {
        CqlSession mockCqlSession = Mockito.mock(CqlSession.class);
        Metadata mockMetadata = Mockito.mock(Metadata.class);
        TokenMap mockTokenMap = Mockito.mock(TokenMap.class);

        Mockito.when(mockCqlSession.getMetadata()).thenReturn(mockMetadata);
        Mockito.when(mockMetadata.getTokenMap()).thenReturn(Optional.of(mockTokenMap));


        Storage storage = new CassandraStorage.Builder()
                .cqlSession(mockCqlSession)
                .batchSize(256)
                .outboxTable(new PseudoTable("",""))
                .build();

        assertEquals(0, storage.getThreadCount(),
                "Поле threadCount должно быть строго равно переданному значению");
    }

    @Test
    void testBuilderWithExplicitThreadCount() throws Exception {
        CqlSession mockCqlSession = Mockito.mock(CqlSession.class);
        Metadata mockMetadata = Mockito.mock(Metadata.class);
        TokenMap mockTokenMap = Mockito.mock(TokenMap.class);

        Mockito.when(mockCqlSession.getMetadata()).thenReturn(mockMetadata);
        Mockito.when(mockMetadata.getTokenMap()).thenReturn(Optional.of(mockTokenMap));

        int expectedThreadCount = 7;

        Storage storage = new CassandraStorage.Builder()
                .cqlSession(mockCqlSession)
                .batchSize(256)
                .threadCount(expectedThreadCount)
                .outboxTable(new PseudoTable("",""))
                .build();

        assertEquals(expectedThreadCount, storage.getThreadCount(),
                "Поле threadCount должно быть строго равно переданному значению");
    }

    @Test
    void testBuilderWithDefaultThreadCountFromConfig() throws Exception {
        CqlSession mockCqlSession = Mockito.mock(CqlSession.class);
        Metadata mockMetadata = Mockito.mock(Metadata.class);
        TokenMap mockTokenMap = Mockito.mock(TokenMap.class);

        Mockito.when(mockCqlSession.getMetadata()).thenReturn(mockMetadata);
        Mockito.when(mockMetadata.getTokenMap()).thenReturn(Optional.of(mockTokenMap));

        DriverContext mockContext = Mockito.mock(DriverContext.class);
        DriverConfigLoader mockConfigLoader = Mockito.mock(DriverConfigLoader.class);
        DriverConfig mockConfig = Mockito.mock(DriverConfig.class);
        DriverExecutionProfile mockProfile = Mockito.mock(DriverExecutionProfile.class);

        int expectedThreadCount = 7;

        Mockito.when(mockCqlSession.getContext()).thenReturn(mockContext);
        Mockito.when(mockContext.getConfigLoader()).thenReturn(mockConfigLoader);
        Mockito.when(mockConfigLoader.getInitialConfig()).thenReturn(mockConfig);
        Mockito.when(mockConfig.getDefaultProfile()).thenReturn(mockProfile);

        Mockito.when(mockProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
                .thenReturn(expectedThreadCount);

        Storage storage = new CassandraStorage.Builder()
                .cqlSession(mockCqlSession)
                .batchSize(256)
                .outboxTable(new PseudoTable("", ""))
                .build();

        assertEquals(expectedThreadCount, storage.getThreadCount(),
                "Поле threadCount должно автоматически инициализироваться из настроек CONNECTION_POOL_LOCAL_SIZE");
    }
}
