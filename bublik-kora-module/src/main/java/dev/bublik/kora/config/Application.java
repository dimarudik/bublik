package dev.bublik.kora.config;

import io.koraframework.application.graph.InitializedGraph;
import io.koraframework.application.graph.KoraApplication;
import io.koraframework.application.graph.Node;
import io.koraframework.common.annotation.KoraApp;
import io.koraframework.config.hocon.HoconConfigModule;

@KoraApp
public interface Application extends HoconConfigModule, KoraBublikModule {

    static void main(String[] args) {
/*
        // Принудительно заставляем Kora читать наш конфиг из src/main/resources/application.conf
        System.setProperty("config.resource", "application.conf");

        System.out.println(ApplicationGraph.graph().getRoot().getSimpleName());
        ApplicationGraph.graph().getNodes().forEach(
                node -> System.out.println("Node: " + node.getClass().getSimpleName())
        );
        InitializedGraph graph = ApplicationGraph.graph().init();
        (Node<KoraBublikExecutor>) graph.findNode(KoraBublikExecutor.class);
        graph.get(KoraBublikExecutor.class);
        KoraApplication.run(ApplicationGraph::graph);
*/
    }
}
