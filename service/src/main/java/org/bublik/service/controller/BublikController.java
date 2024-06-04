package org.bublik.service.controller;

import org.bublik.Bublik;
import org.bublik.model.Config;
import org.bublik.service.config.ConnectionConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class BublikController {
    private final ConnectionConfig connectionConfig;

    @Autowired
    public BublikController(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    // https://medium.com/@electronicelif/asynchronous-programming-with-spring-e07be2a19cfc

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @Async
    public void startBublik(@RequestBody List<Config> configs) {
        Bublik bublik = Bublik.getInstance(connectionConfig.getConnectionProperty(), configs);
        bublik.start();
    }
}
