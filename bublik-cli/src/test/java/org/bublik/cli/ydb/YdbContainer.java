package org.bublik.cli.ydb;

import lombok.NonNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.Future;

public class YdbContainer extends JdbcDatabaseContainer<YdbContainer>  {
    public YdbContainer(@NonNull Future<String> image) {
        super(image);
    }

    public YdbContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @Override
    public String getDriverClassName() {
        return "";
    }

    @Override
    public String getJdbcUrl() {
        return "";
    }

    @Override
    public String getUsername() {
        return "";
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    protected String getTestQueryString() {
        return "";
    }
}
