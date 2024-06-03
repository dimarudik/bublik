package org.bublik.model;

import java.sql.SQLException;

public record RunnerResult (LogMessage logMessage, SQLException e) {}
