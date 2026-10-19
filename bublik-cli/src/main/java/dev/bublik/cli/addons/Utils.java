package dev.bublik.cli.addons;

import dev.bublik.core.model.ConnectionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static ConnectionProperty connectionProperty(String configFileName) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = new FileInputStream(configFileName)) {
            return yaml.loadAs(in, ConnectionProperty.class);
        }
    }

    public static ConnectionProperty connectionProperty(InputStream is) throws IOException {
        Yaml yaml = new Yaml();
        return yaml.loadAs(is, ConnectionProperty.class);
    }
}
