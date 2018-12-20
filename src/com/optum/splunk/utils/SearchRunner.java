package com.optum.splunk.utils;

import com.splunk.*;
import com.splunk.Args;
import com.splunk.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by Greg Zillgitt on 8/26/2016.
 */
public class SearchRunner {
    private static Logger logger = LoggerFactory.getLogger(SearchRunner.class);


    private static String getSearchQueryFromFile(String fn) throws Exception {
        logger.debug("getSearchFromFile(): opening " + fn);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fn)));
        logger.debug("getSearchFromFile(): reading " + fn);
        String s = "";
        while (true) {
            String line = br.readLine();
            if (line == null)
                break;
            if ((line.trim().length() > 0) && (line.trim().substring(0,1).equals("#")))
                continue;
            s += line + " ";
            //logger.debug(s);
        }
        br.close();
        if (s.equals("")) {
            logger.error("Search file specified but search string not found");
            return null;
        }
        if (!s.toLowerCase().startsWith("search "))
            s = "search " + s;
        logger.debug("getSearchFromFile(): got search [" + s + "]");
        return s;
    }


    private static void addCLIRules(Command command) {
        String appContextText = "Splunk app context (default: search)";
        String earliestTimeText = "Search earliest time";
        String dequoteText = "Remove surrounding double-quotes from output";
        String hostText = "Override Splunk host (default: from .splunkrc)";
        String latestTimeText = "Search latest time";
        String logFileText = "Log file path (default: none)";
        String outFileText = "Output file path";
        String outputModeText =
                "Search output format {csv, raw, json, xml} (default: csv)";
        String resultsCountText =
                "The maximum number of results to return (default: unlimited)";
        String searchFileText = "Path of file containing search string";
        String userString = "Splunk user";
        String passwordString = "Splunk password";
        String verboseString = "Display search progress";

        command.addRule("app", String.class, appContextText);
        command.addRule("host", String.class, hostText);
        command.addRule("count", Integer.class, resultsCountText);
        command.addRule("dequote", String.class, dequoteText);
        command.addRule("earliest", String.class, earliestTimeText);
        command.addRule("latest", String.class, latestTimeText);
        command.addRule("logfile", String.class, logFileText);
        command.addRule("outfile", String.class, outFileText);
        command.addRule("omode", String.class, outputModeText);
        command.addRule("searchfile", String.class, searchFileText);
        command.addRule("verbose",  verboseString);
        command.addRule("user",  String.class, userString);
        command.addRule("password",  String.class, passwordString);
    }


    private static String getCLIOption(Command command, String optionName, String defaultValue) {
        String s = defaultValue;
        if (command.opts.containsKey(optionName))
            s = (String)command.opts.get(optionName);
        if (!optionName.equals("loglevel"))
            logger.debug(optionName + "="+s);
        return s;
    }


    public static void main(String[] args) throws Exception {

        logger.info("Splunk search started...");

        logger.info("Parsing command line options");
        Command command = Command.splunk("search");
        addCLIRules(command);
        command.parse(args);

        boolean verbose = command.opts.containsKey("verbose");
        logger.debug("verbose=" + verbose);

        boolean dequote = command.opts.containsKey("dequote");
        logger.debug("dequote=" + dequote);

        //String appContext = getCLIOption(command, "app", "search");
        String earliestTime = getCLIOption(command, "earliest", null);
        String host = getCLIOption(command, "host", null);
        String latestTime = getCLIOption(command, "latest", null);
        String outFileName = getCLIOption(command, "outfile", null);
        String outputMode = getCLIOption(command, "omode", "csv");
        String searchFileName = getCLIOption(command, "searchfile", null);
        String user = getCLIOption(command, "user", null);
        String password = getCLIOption(command, "password", null);

        String query;
        if (searchFileName == null) {
            if (command.args.length == 0)
                Command.error("Search expression required");
            query = command.args[0];
        }
        else {
            logger.debug("Getting search query from " + searchFileName);
            query = getSearchQueryFromFile(searchFileName);
            if (query == null) {
                Command.error("No search string found in " + searchFileName);
            }
        }
        logger.debug("query=" + query);

        Search search = new Search.SearchBuilder()
                .earliestTime(earliestTime)
                .latestTime(latestTime)
                .host(host)
                .outputMode(outputMode)
                .outFileName(outFileName)
                .user(user)
                .password(password)
                .query(query)
                .build();

        search.run();

    }

}
