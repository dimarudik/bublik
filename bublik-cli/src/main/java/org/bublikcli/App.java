package org.bublikcli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.bublik.constants.ENVProperties;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.service.StorageService;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import static org.bublik.exception.Utils.getStackTrace;
import static org.bublikcli.addons.Utils.*;
import static org.bublikcli.constants.StringConstant.HELP_MESSAGE;

/*
java -cp ./chekist/target/chekist-1.0-SNAPSHOT.jar:./cli/target/bublik-cli-1.2.0.jar org.bublikcli.App -k 1000 -c ./cli/config/pg2pg-sec.yaml -m ./cli/config/pg2pg-sec.json
*/

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public void methodA() throws IOException {
        final Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("project.properties"));
        log.info("version : {}", properties.getProperty("version"));
//        System.out.println(properties.getProperty("version"));
//        System.out.println(properties.getProperty("artifactId"));
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
//        options.addOption("?", "help", false, "help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            Arrays.stream(cmd.getOptions()).forEach(option -> log.info("-{} {}",
                    option.getOpt(), option.getValue() == null ? "" : option.getValue()));
        } catch (ParseException e) {
//            log.error(e.getMessage(), e);
//            formatter.printHelp( HELP_MESSAGE, options );
            return;
        }

        if (cmd.hasOption(helpOption)) {
            formatter.printHelp( HELP_MESSAGE, options );
        }/* else if (cmd.hasOption(SyncOption) && cmd.hasOption("c") && !cmd.hasOption(createChunkOption) && !cmd.hasOption("m")) {
            sync(cmd.getOptionValue(connectionConfigOption));
        }*/ else if(cmd.hasOption("m") && cmd.hasOption("g") && cmd.hasOption("n")) {
            createOGGFile(cmd.getOptionValue(mappingDefOption), cmd.getOptionValue(OGGfileOption), cmd.getOptionValue(OGGCSNOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("i") && cmd.hasOption("o")) {
            createDefJson(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(listOfTablesOption), cmd.getOptionValue(JSONfileOption));
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            // how to run without chunk creation
            run(cmd.getOptionValue(mappingDefOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            // how to run without chunk creation
            run(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(mappingDefOption));
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation from ENV
            run(cmd.getOptionValue(mappingDefOption), Integer.parseInt(cmd.getOptionValue(createChunkOption)),
                    cmd.hasOption(SyncOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation from yaml config file
            run(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(mappingDefOption),
                    Integer.parseInt(cmd.getOptionValue(createChunkOption)), cmd.hasOption(SyncOption));
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

    private static void run(String mappingDefFileName) {
        run(mappingDefFileName,0, false);
    }

    private static void run(String mappingDefFileName, int rowsParameter, boolean sync) {
        ConnectionProperty connectionProperty = envConnectionProperty();
        runProcess(connectionProperty, mappingDefFileName, rowsParameter, sync);
    }

    private static void run(String configFileName, String mappingDefFileName) {
        run(configFileName, mappingDefFileName, 0, false);
    }

    private static void run(String configFileName, String mappingDefFileName, int rowsParameter, boolean sync) {
        try {
            ConnectionProperty properties = connectionProperty(configFileName);
            runProcess(properties, mappingDefFileName, rowsParameter, sync);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    private static void runProcess(ConnectionProperty connectionProperty,
                                   String mappingDefFileName,
                                   int rowsParameter,
                                   boolean sync) {
        try {
            log.info("THREADS: {}", connectionProperty.getThreadCount());
            log.info("SOURCE: {}", connectionProperty.getFromProperty().getProperty("url"));
            log.info("SOURCE USERNAME: {}", connectionProperty.getFromProperty().getProperty("user"));
            ObjectMapper mapperJSON = new ObjectMapper();
            List<Config> configs =
                    List.of(mapperJSON.readValue(Paths.get(mappingDefFileName).toFile(),
                            Config[].class));
//            createChunks(connectionProperty, rowsParameter, configs, sync);
            try {
                log.info("Bublik starting...");
                Storage sourceStorage = StorageService.getStorage(connectionProperty.getFromProperty(), connectionProperty, true);
                assert sourceStorage != null;
                sourceStorage.start(configs, sync, rowsParameter);
//                log.info("All Bublik's tasks have been done. \u001B[31mYou can create all needed indexes on target tables now.\u001B[0m");
            } catch (SQLException e) {
                log.error("{}", getStackTrace(e));
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

/*
    private static void createChunks(ConnectionProperty connectionProperty, int rowsParameter, List<Config> config, boolean sync) {
        if (rowsParameter == 0) {
            log.info("No rows parameter provided, skipping chunk creation.");
            return;
        }
        try {
            Connection fromConnection = DriverManager.getConnection(connectionProperty.getFromProperty().getProperty("url"),
                    connectionProperty.getFromProperty());
            fromConnection.setAutoCommit(false);
            Driver fromDriver = DriverManager.getDriver(connectionProperty.getFromProperty().getProperty("url"));
            switch (fromDriver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" -> fillOraChunks(config, fromConnection, rowsParameter);
                case "org.postgresql.Driver" ->  {
                    if (sync) {
                    } else {
                        fillCtidChunksV2(config, fromConnection, rowsParameter, false);
                    }
                }
                default -> throw new RuntimeException();
            }
            fromConnection.close();

            Driver toDriver = DriverManager.getDriver(connectionProperty.getToProperty().getProperty("url"));
            log.info("TARGET: {}", connectionProperty.getToProperty().getProperty("url"));
            log.info("TARGET USERNAME: {}", connectionProperty.getToProperty().getProperty("user"));
            Connection toConnection = DriverManager.getConnection(connectionProperty.getToProperty().getProperty("url"),
                    connectionProperty.getToProperty());
            toConnection.setAutoCommit(false);
            switch (toDriver.getClass().getName()) {
                case "org.postgresql.Driver" :
                    createPostgreSQLTableBublikChunk(toConnection);
                    break;
                case "tech.ydb.jdbc.YdbDriver" :
                    createYDBTableBublikChunk(toConnection);
                    break;
            }
            toConnection.close();
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }
*/

    private static ConnectionProperty envConnectionProperty() {
        ENVProperties[] e = ENVProperties.values();
        Map<String, String> envStrings = System.getenv();
        Map<String, String> fromENVMap = new HashMap<>();
        Map<String, String> toENVMap = new HashMap<>();
        ConnectionProperty connectionProperty = new ConnectionProperty();
        for (ENVProperties env : e) {
            if (envStrings.containsKey(env.name())) {
                switch (env) {
                    case THREAD_COUNT -> connectionProperty.setThreadCount(Integer.parseInt(envStrings.get(env.name())));
                    case FROM_URL -> fromENVMap.put("url", envStrings.get(env.name()));
                    case FROM_USER -> fromENVMap.put("user", envStrings.get(env.name()));
                    case FROM_PASSWORD -> fromENVMap.put("password", envStrings.get(env.name()));
                    case TO_URL -> toENVMap.put("url", envStrings.get(env.name()));
                    case TO_USER -> toENVMap.put("user", envStrings.get(env.name()));
                    case TO_PASSWORD -> toENVMap.put("password", envStrings.get(env.name()));
                }
            }
        }
        connectionProperty.setFromProperties(fromENVMap);
        connectionProperty.setToProperties(toENVMap);
        return connectionProperty;
    }
}
