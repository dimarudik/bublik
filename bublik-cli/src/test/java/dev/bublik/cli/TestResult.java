package dev.bublik.cli;

import dev.bublik.core.model.Storage2Storage;

public record TestResult(
        long sourceCount,
        long targetCount,
        Storage2Storage storage2Storage
){}
