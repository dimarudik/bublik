package org.bublik.core.service.controller;

import org.bublik.core.model.Config;
import org.bublik.core.service.StorageService;
import org.bublik.core.service.config.ConnectionConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;

@RestController
public class BublikController {
    private final ConnectionConfig connectionConfig;

    @Autowired
    public BublikController(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public void startBublik(@RequestBody List<Config> configs) throws SQLException {
        StorageService.init(connectionConfig.getConnectionProperty(), configs, false, 50000);
    }
}
