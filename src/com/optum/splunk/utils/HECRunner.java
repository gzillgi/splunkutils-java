package com.optum.splunk.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * HEC wrapper - intenxded for testing and quick & dirty adhoc stuff
 *
 * @author Greg Zillgitt
 */
public class HECRunner {

    private static String protocol = "http:"; // http: or https:
    private static String server = "apsrd3220.uhc.com";
    private static String port = "9992";
    private static String endpoint = "services/collector/raw/1.0";
    private static String destinationURL;
    private static String token = "";
    private static String index = "";
    private static String source = "";
    private static String sourceType = "";
    private static Logger logger = LoggerFactory.getLogger(HECRunner.class);


    private static void loadProperties () {
        Properties p = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("splunkutils.properties");
            if (is.available() > 0) {
                p.load(is);
                protocol = p.getProperty("hec-protocol", protocol);
                server = p.getProperty("hec-server", server);
                port = p.getProperty("hec-port", port);
                endpoint = p.getProperty("hec-endpoint", endpoint);
                token = p.getProperty("hec-token", token);
                index = p.getProperty("hec-index", index);
                source = p.getProperty("hec-source", source);
                sourceType = p.getProperty("hec-sourcetype", sourceType);
                logger.debug("splunkutils.properties loaded");
            }
            else {
                logger.warn("splunkutils.properties found but was empty or unreadable");
            }
        }
        catch (IOException e) {
            logger.warn("splunkutils.properties not found; using parameters and defaults");
        }
     }

    private static String sendFileOfEvents(String path) {
        if (StringUtils.isEmpty(path)) {
            logger.error("sendFileOfEvents called with null or empty path parameter");
            return "error";
        }
        HEC hec = new HEC.HECBuilder()
                .source(source)
                .sourceType(sourceType)
                .index(index)
                .token(token)
                .destinationURL(destinationURL)
                .logger(logger)
                .build();
        logger.debug("Sending file of events; hecr: " + hec.toString());
        String result;
        try {
            result = hec.sendFileOfEvents(path);
        }
        catch (IOException e) {
            result = e.getMessage();
            logger.error("Error sending file: " + result);
        }
        return result;
    }


    public static void main(String[] args) {

        logger.debug("classpath=" + System.getProperty("java.class.path"));
        logger.debug("current working dir=" + Paths.get(".").toAbsolutePath().normalize().toString());

        loadProperties();

        CommandLine commandLine;
        Option option_file = Option.builder("file").argName("file_arg").hasArg().desc("File").build();
        Option option_source = Option.builder("source").argName("source_arg").hasArg().desc("Source").build();
        Option option_token = Option.builder("token").argName("token_arg").hasArg().desc("Token").build();
        Option option_index = Option.builder("index").argName("index_arg").hasArg().desc("Index").build();
        Option option_url = Option.builder("url").argName("url_arg").hasArg().desc("URL").build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(option_file);
        options.addOption(option_source);
        options.addOption(option_token);
        options.addOption(option_index);
        options.addOption(option_url);

        String path = "";

        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("file")) {
                path = commandLine.getOptionValue("file");
                logger.info("Input file path: " + path);
            }
            if (commandLine.hasOption("source")) {
                source =  commandLine.getOptionValue("source");
                logger.info("Source override=" + source);
            }
            if (commandLine.hasOption("index")) {
                index = commandLine.getOptionValue("index");
                logger.info("Index override=" + index);
            }
            if (commandLine.hasOption("token")) {
                token = commandLine.getOptionValue("token");
                logger.info("Token override=" + token);
            }
            if (commandLine.hasOption("url")) {
                destinationURL = commandLine.getOptionValue("url");
                logger.info("Dest URL=" + destinationURL);
            }

        }
        catch (ParseException e) {
            logger.error("Options parse error: ");
            logger.error(e.getMessage());
            System.exit(2);
        }

        if ((path.length() > 0) && Files.exists(Paths.get(path)) && Files.isReadable(Paths.get(path))) {
            logger.info("Sending file: [" + path + "]");
            if (!protocol.endsWith(":"))
                protocol += ":";
            destinationURL = protocol + "//" + server + ":" + port + "/" + endpoint;
            String response = sendFileOfEvents(path);
            logger.info("response: " + response);
        }
        else {
            logger.error("No valid file to process");
        }

        logger.info("HECRunner finished.");

    }

}
