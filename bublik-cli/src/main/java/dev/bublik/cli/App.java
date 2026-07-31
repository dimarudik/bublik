package dev.bublik.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.bublik.core.constants.ENVProperties;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
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
import static dev.bublik.cli.constants.StringConstant.HELP_MESSAGE;
import static dev.bublik.core.util.Utils.getStackTrace;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

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
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(configs);
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            // how to run without chunk creation
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(cmd.getOptionValue(connectionConfigOption), configs);
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation from ENV
            List<Config> configs = getConfigs(cmd.getOptionValue(mappingDefOption));
            run(configs, Integer.parseInt(cmd.getOptionValue(createChunkOption)));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation from yaml config file
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

    private static void run(String configFileName, List<Config> configs, int rowsParameter) {
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
