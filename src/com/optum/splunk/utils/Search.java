package com.optum.splunk.utils;

import com.splunk.*;
import com.splunk.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by gzillgi on 8/26/2016.
 */
public class Search {

    String query;
    String earliestTime;
    String host;
    String latestTime;
    String outFileName;
    String outputMode;
    String user;
    String password;
    boolean verbose;

    private static Logger logger = LoggerFactory.getLogger(Search.class);


    public static class SearchBuilder {
        private String query;
        private String earliestTime = "-1d@d";
        private String latestTime = "@d";
        private String host = "myserver";
        private String outFileName = "search.csv";
        private String outputMode = "csv";
        private String user;
        private String password;
        private Logger logger;

        public SearchBuilder query(String query) {
            this.query = query;
            return this;
        }

        public SearchBuilder earliestTime(String earliestTime) {
            this.earliestTime = earliestTime;
            return this;
        }

        public SearchBuilder latestTime(String latestTime) {
            this.latestTime = latestTime;
            return this;
        }

        public SearchBuilder host(String host) {
            this.host = host;
            return this;
        }

        public SearchBuilder outFileName(String outFileName) {
            this.outFileName = outFileName;
            return this;
        }

        public SearchBuilder outputMode(String outputMode) {
            this.outputMode = outputMode;
            return this;
        }

        public SearchBuilder user(String user) {
            this.user = user;
            return this;
        }

        public SearchBuilder password(String password) {
            this.password = password;
            return this;
        }

        public SearchBuilder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Search build() {
            if (this.logger == null) {
                this.logger = LoggerFactory.getLogger(HEC.class);
            }
            return new Search(this);
        }

    } // SearchBuilder


    private Search(SearchBuilder builder) {
        this.query = builder.query;
        this.earliestTime = builder.earliestTime;
        this.host = builder.host;
        this.latestTime = builder.latestTime;
        this.outFileName = builder.outFileName;
        this.outputMode = builder.outputMode;
        this.user = builder.user;
        this.password = builder.password;
    }


    private  void logJavaProperties() {
        logger.debug("java version = " + System.getProperty("java.version"));
        logger.debug("java home = " + System.getProperty("java.home"));
        logger.debug("classpath = " + System.getProperty("java.class.path"));
        logger.debug("library path = " + System.getProperty("java.library.path"));
        logger.debug("user name = " + System.getProperty("user.name"));
        logger.debug("os name = " + System.getProperty("os.name"));
        logger.debug("os arch = " + System.getProperty("os.arch"));
        logger.debug("os version = " + System.getProperty("os.version"));
    }


    public void run() throws Exception {

        logger.debug("run()");
        logger.debug("query="+query);

        logger.debug("Connecting to Splunk...");
        Command command = Command.splunk("search");
        HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
        Service service = Service.connect(command.opts);

        // Check the syntax of the query.
        logger.debug("Parsing search...");
        try {
            service.parse(query, new Args("parse_only", true));
        }
        catch (HttpException e) {
            Command.error("query '%s' is invalid: %s", query, e.getDetail());
        }

        // Create a search job for the given query & query arguments.
        Args queryArgs = new Args();
        if (StringUtils.isNotBlank(earliestTime))
            queryArgs.put("earliest_time", earliestTime);

        if (StringUtils.isNotBlank(latestTime))
            queryArgs.put("latest_time", latestTime);

        if (StringUtils.isNotBlank(host))
            queryArgs.put("host", host);

        if (StringUtils.isNotBlank(user))
            queryArgs.put("splunk_user", user);

        if (StringUtils.isNotBlank(password))
            queryArgs.put("password", password);

        logger.debug("Splunk host: " + queryArgs.get("host"));
        logger.debug("earliest: " + queryArgs.get("earliest_time"));
        logger.debug("latest: " + queryArgs.get("latest_time"));
        logger.debug("user: " + queryArgs.get("splunk_user"));

        logger.debug("Creating search job...");
        Job job = service.getJobs().create(query, queryArgs);

        // Wait until results are available.
        boolean didPrintAStatusLine = false;
        while (!job.isDone()) {
            Thread.sleep(1000);
        }

        int resultCount = job.getResultCount();
        int eventCount = job.getEventCount();
        int scanCount = job.getScanCount();
        logger.debug("Job stats: # results = " + resultCount + "; # events = " + eventCount + "; # scanned = " + scanCount);

        if (resultCount == 0)
            return;

        Args outputArgs = new Args();
        outputArgs.put("count", 0);
        outputArgs.put("offset", 0);
        outputArgs.put("output_mode", outputMode);

        logger.debug("Opening results stream...");
        InputStream stream;
        stream = job.getResults(outputArgs);

        logger.debug("Opening output stream...");
        java.io.PrintStream outFile;
        if (StringUtils.isNotBlank(outFileName)) {
            outFile = new java.io.PrintStream(outFileName);
            logger.debug("writing output to " + outFileName);
        }
        else {
            outFile = System.out;
            logger.debug("writing output to stdout");
        }

        logger.debug("Writing search results; outputMode=[" + outputMode +"]");
        InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        try {
            OutputStreamWriter writer = new OutputStreamWriter(outFile);
            try {
                int size = 1024;
                char[] buffer = new char[size];
                while (true) {
                    int count = reader.read(buffer);
                    if (count == -1) break;
                    writer.write(buffer, 0, count);
                }

                writer.write("\n");
            }
            finally {
                writer.close();
            }
        }
        finally {
            reader.close();
        }

        logger.debug("Closing search job...");
        job.cancel();
        logger.debug("Splunk search complete");

    } // run

} // Search
