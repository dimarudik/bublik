package dev.bublik.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.bublik.core.constants.ENVProperties;
import dev.bublik.core.constants.TransferMode;
import dev.bublik.core.model.*;
import dev.bublik.core.service.StorageService;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import static dev.bublik.cli.addons.Utils.*;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final String HELP_MESSAGE = "bublik-cli [options]";

    public void methodA() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("project.properties")) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                log.info("version : {}", properties.getProperty("version"));
            } else {
                log.info("version : unknown (local dev mode)");
            }
        } catch (IOException e) {
            log.warn("Could not read project.properties: {}", e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        new App().methodA();

        Options options = new Options();
        Option createChunkOption = createOptionValue("k", "chunk", "rows number", "create chunks at source");
        Option connectionConfigOption = createOptionValue("c", "config", "yaml file", "file name of connection properties");
        Option mappingDefOption = createOptionValue("m", "mapping-definitions", "json file", "file name with mapping definitions of tables");
        Option helpOption = createOptionNoArg("h", "help", "Help message");
        Option modeOption = createOptionValue("s", "mode", "COPY|UPSERT", "transfer mode execution context (default: COPY)");

        options
                .addOption(createChunkOption)
                .addOption(connectionConfigOption)
                .addOption(mappingDefOption)
                .addOption(helpOption)
                .addOption(modeOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            Arrays.stream(cmd.getOptions()).forEach(option -> log.info("-{} {}",
                    option.getOpt(), option.getValue() == null ? "" : option.getValue()));
        } catch (ParseException e) {
            formatter.printHelp(HELP_MESSAGE, options);
            return;
        }

        if (cmd.hasOption(helpOption)) {
            formatter.printHelp(HELP_MESSAGE, options);
            return;
        }

        if (cmd.hasOption(mappingDefOption)) {
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));

            int rowsParameter = cmd.hasOption(createChunkOption)
                    ? Integer.parseInt(cmd.getOptionValue(createChunkOption))
                    : 0;

            TransferMode transferMode = TransferMode.COPY;
            if (cmd.hasOption(modeOption)) {
                try {
                    transferMode = TransferMode.valueOf(cmd.getOptionValue(modeOption).toUpperCase());
                } catch (IllegalArgumentException e) {
                    log.error("Unsupported execution transfer mode: {}. Allowed configurations: COPY, UPSERT", cmd.getOptionValue(modeOption));
                    return;
                }
            }

            ConnectionProperty properties;
            if (cmd.hasOption(connectionConfigOption)) {
                try {
                    properties = connectionProperty(cmd.getOptionValue(connectionConfigOption));
                } catch (Exception e) {
                    log.error("Error reading configuration architecture from source file {}: {}", cmd.getOptionValue(connectionConfigOption), e.getMessage());
                    return;
                }
            } else {
                properties = envConnectionProperty();
            }

            runProcess(properties, configs, rowsParameter, transferMode);

        } else {
            formatter.printHelp(HELP_MESSAGE, options);
        }
    }

    public static Storage2Storage runProcess(ConnectionProperty property,
                                  List<Config> configs,
                                  int rowsParameter) {
        return runProcess(property, configs, rowsParameter, TransferMode.COPY);
    }

    public static Storage2Storage runProcess(ConnectionProperty property,
                                             List<Config> configs,
                                             int rowsParameter,
                                             TransferMode mode) {
        try {
            Table chunkTable = null;
            Table outboxTable = null;
            if (property.getFromProperty().containsKey("chunkSchemaName") && property.getFromProperty().containsKey("chunkTableName")) {
                chunkTable = new DummyTable((String) property.getFromProperty().get("chunkSchemaName"),
                        (String) property.getFromProperty().get("chunkTableName"));
            }
            if (property.getToProperty().containsKey("outboxSchemaName") && property.getToProperty().containsKey("outboxTableName")) {
                outboxTable = new DummyTable((String) property.getToProperty().get("outboxSchemaName"),
                        (String) property.getToProperty().get("outboxTableName"));
            }
            return StorageService.init(property, configs, rowsParameter, chunkTable, outboxTable, mode);
//            StorageService.init(property, configs, rowsParameter, mode);
        } catch (SQLException e) {
            log.error("SQL Engine Fail [{}]: {}", e.getSQLState(), e.getMessage());
        } catch (Exception r) {
            log.error("Pipeline breakdown failure context", r);
        }
        return null;
    }

    private static Option createOptionNoArg(String shortName, String longName, String description) {
        return Option.builder(shortName).longOpt(longName).desc(description).required(false).build();
    }

    private static Option createOptionValue(String shortName, String longName, String argName, String description) {
        return Option.builder(shortName).longOpt(longName).argName(argName).desc(description).hasArg().required(false).build();
    }

    public static List<Config> getConfigs(String mappingDefFileName) throws IOException {
        ObjectMapper mapperJSON = new ObjectMapper();
        return List.of(mapperJSON.readValue(Paths.get(mappingDefFileName).toFile(), Config[].class));
    }

    public static List<Config> getConfigs(InputStream is) throws IOException {
        ObjectMapper mapperJSON = new ObjectMapper();
        return List.of(mapperJSON.readValue(is, Config[].class));
    }

    private static ConnectionProperty envConnectionProperty() {
        ENVProperties[] e = ENVProperties.values();
        Map<String, String> envStrings = System.getenv();
        Map<String, String> fromENVMap = new HashMap<>();
        Map<String, String> toENVMap = new HashMap<>();

        int parsedThreadCount = 2;

        for (ENVProperties env : e) {
            if (envStrings.containsKey(env.name())) {
                String envValue = envStrings.get(env.name());
                switch (env) {
                    case THREAD_COUNT -> {
                        try {
                            parsedThreadCount = Integer.parseInt(envValue);
                        } catch (NumberFormatException ex) {
                            log.warn("Cannot parse THREAD_COUNT from '{}' is not a number. Setting default : {}", envValue, parsedThreadCount);
                        }
                    }
                    case FROM_URL -> fromENVMap.put("url", envValue);
                    case FROM_USER -> fromENVMap.put("user", envValue);
                    case FROM_PASSWORD -> fromENVMap.put("password", envValue);
                    case FROM_FETCH_SIZE -> fromENVMap.put("fetchSize", envValue);
                    case FROM_CHUNK_SCHEMA_NAME -> fromENVMap.put("chunkSchemaName", envValue);
                    case FROM_CHUNK_TABLE_NAME -> fromENVMap.put("chunkTableName", envValue);
                    case TO_URL -> toENVMap.put("url", envValue);
                    case TO_USER -> toENVMap.put("user", envValue);
                    case TO_PASSWORD -> toENVMap.put("password", envValue);
                    case TO_OUTBOX_SCHEMA_NAME -> toENVMap.put("outboxSchemaName", envValue);
                    case TO_OUTBOX_TABLE_NAME -> toENVMap.put("outboxTableName", envValue);
                }
            }
        }

        return new ConnectionProperty.Builder()
                .threadCount(parsedThreadCount)
                .fromProperties(fromENVMap)
                .toProperties(toENVMap)
                .build();
    }

/*
    public void methodA() throws IOException {
        final Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("project.properties"));
        log.info("version : {}", properties.getProperty("version"));
    }


    public static void main(String[] args) throws IOException, SQLException {

        new App().methodA();

        Options options = new Options();
        Option createChunkOption = createOptionValue("k", "chunk", "rows number","create ctid chunks at source");
        Option connectionConfigOption = createOptionValue("c", "config", "yaml file", "file name of prop.erties");
        Option mappingDefOption = createOptionValue("m", "mapping-definitions", "json file", "file name with mapping definitions of tables");
        Option listOfTablesOption = createOptionValue("i", "init", "json file", "file name with a list of tables");
        Option JSONfileOption = createOptionValue("o", "output", "json file", "create new mapping definitions file");
        Option OGGfileOption = createOptionValue("g", "ogg", "ogg file", "create Oracle Golden Gate file");
        Option OGGCSNOption = createOptionValue("n", "csn", "csn", "Oracle Golden Gate CSN");
        Option SyncOption = createOptionNoArg("s", "sync", "synchronize data from source to target");
        Option helpOption = createOptionNoArg("h", "help", "Help message");

        options
                .addOption(createChunkOption)
                .addOption(connectionConfigOption)
                .addOption(mappingDefOption)
                .addOption(listOfTablesOption)
                .addOption(JSONfileOption)
                .addOption(SyncOption)
                .addOption(OGGfileOption)
                .addOption(OGGCSNOption)
                .addOption(helpOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            Arrays.stream(cmd.getOptions()).forEach(option -> log.info("-{} {}",
                    option.getOpt(), option.getValue() == null ? "" : option.getValue()));
        } catch (ParseException e) {
//            formatter.printHelp( HELP_MESSAGE, options );
            return;
        }

        if (cmd.hasOption(helpOption)) {
            formatter.printHelp( HELP_MESSAGE, options );
        } else if(cmd.hasOption("m") && cmd.hasOption("g") && cmd.hasOption("n")) {
            createOGGFile(cmd.getOptionValue(mappingDefOption), cmd.getOptionValue(OGGfileOption), cmd.getOptionValue(OGGCSNOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("i") && cmd.hasOption("o")) {
            createDefJson(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(listOfTablesOption), cmd.getOptionValue(JSONfileOption));
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(configs);
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(cmd.getOptionValue(connectionConfigOption), configs);
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(configs, Integer.parseInt(cmd.getOptionValue(createChunkOption)));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(cmd.getOptionValue(connectionConfigOption), configs,
                    Integer.parseInt(cmd.getOptionValue(createChunkOption)));
        } else {
            formatter.printHelp( HELP_MESSAGE, options );
        }
    }

    private static Option createOptionNoArg(String shortName, String longName, String description) {
        return Option.builder(shortName)
                .longOpt(longName)
                .desc(description)
                .required(false)
                .build();
    }

    private static Option createOptionValue(String shortName, String longName, String argName, String description) {
        return Option.builder(shortName)
                .longOpt(longName)
                .argName(argName)
                .desc(description)
                .hasArg()
                .required(false)
                .build();
    }

    private static void run(List<Config> configs) {
        run(configs,0);
    }

    private static void run(List<Config> configs, int rowsParameter) {
        ConnectionProperty connectionProperty = envConnectionProperty();
        runProcess(connectionProperty, configs, rowsParameter);
    }

    private static void run(String configFileName, List<Config> configs) {
        run(configFileName, configs, 0);
    }

    private static void run(String configFileName,
                            List<Config> configs,
                            int rowsParameter) {
        try {
            ConnectionProperty properties = connectionProperty(configFileName);
            runProcess(properties, configs, rowsParameter);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    public static void runProcess(ConnectionProperty property,
                                  List<Config> configs,
                                  int rowsParameter) {
        try {
            StorageService.init(property, configs, rowsParameter);
        } catch (SQLException e) {
            log.error("{} {}", e.getSQLState(), getStackTrace(e));
        } catch (Exception r) {
            log.error("{}", getStackTrace(r));
        }
    }

    public static List<Config> getConfigs(String mappingDefFileName) throws IOException {
        ObjectMapper mapperJSON = new ObjectMapper();
        return List.of(mapperJSON.readValue(Paths.get(mappingDefFileName).toFile(),
                Config[].class));
    }

    public static List<Config> getConfigs(InputStream is) throws IOException {
        ObjectMapper mapperJSON = new ObjectMapper();
        return List.of(mapperJSON.readValue(is, Config[].class));
    }
*/
}
