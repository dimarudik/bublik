package dev.bublik.starter.autoconfigure;

import dev.bublik.starter.properties.BublikProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BublikStarterTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(BublikAutoConfiguration.class));

    @Test
    void shouldRegisterAutoConfigurationWhenEnabledByDefault() {
        this.contextRunner.run(context -> {
            assertThat(context).hasSingleBean(BublikAutoConfiguration.class);
            assertThat(context).hasSingleBean(BublikProperties.class);

            BublikProperties properties = context.getBean(BublikProperties.class);
            assertThat(properties.isEnabled()).isTrue();
            assertThat(properties.getThreadCount()).isEqualTo(4);
        });
    }

    @Test
    void shouldNotRegisterAutoConfigurationWhenDisabledExplicitly() {
        this.contextRunner
                .withPropertyValues("bublik.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(BublikAutoConfiguration.class);
                    assertThat(context).doesNotHaveBean(BublikProperties.class);
                });
    }

    @Test
    void shouldCorrectlyMapYamlPropertiesToPropertiesClass() {
        this.contextRunner
                .withPropertyValues(
                        "bublik.thread-count=8",
                        "bublik.from.url=jdbc:postgresql://localhost:5432/src",
                        "bublik.to.url=jdbc:postgresql://localhost:5432/dst",
                        "bublik.to-adds.clickhouse.max-connections=20",
                        "bublik.pipelines[0].from-schema=public",
                        "bublik.pipelines[0].from-table=source_users",
                        "bublik.pipelines[0].to-schema=public",
                        "bublik.pipelines[0].to-table=target_users"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(BublikProperties.class);
                    BublikProperties props = context.getBean(BublikProperties.class);

                    // Проверяем базовые типы
                    assertThat(props.getThreadCount()).isEqualTo(8);

                    // Проверяем FROM/TO мапы параметров подключения
                    assertThat(props.getFrom().get("url")).isEqualTo("jdbc:postgresql://localhost:5432/src");

                    // Проверяем нашу сложную вложенную структуру toAdds
                    assertThat(props.getToAdds().get("clickhouse").get("max-connections")).isEqualTo("20");

                    // Проверяем сборку рекордов Config через Билдер Бублика
                    assertThat(props.getPipelines()).hasSize(1);
                    BublikProperties.TablePipelineConfig pipeline = props.getPipelines().get(0);
                    assertThat(pipeline.getFromTable()).isEqualTo("source_users");

                    // Убеждаемся, что конвертация в доменный Config ядра проходит без NPE
                    var coreConfig = pipeline.toRecordConfig();
                    assertThat(coreConfig.fromTableName()).isEqualTo("source_users");
                    assertThat(coreConfig.toTableName()).isEqualTo("target_users");
                });
    }
}
