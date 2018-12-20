package com.optum.splunk.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 *
 *  This Java API provides simple and flexible access to Splunk's HTTP Event Collector (HEC),
 *  which is used to send data (such as application logging) to Splunk.
 *
 *  At a minimum you'll need a token, which is used by Splunk for authentication and authorization. The token is
 *  formatted as a GUID, and is provided to you after your HEC access is configured. You'll also need to know the
 *  URL of the Splunk server that's listening for your HEC traffic.
 *
 *  For an example of how this class can be used see the CLI wrapper "HECRunner" in this package.
 *
 * @author Greg Zillgitt
 * @version 1.0
 * @see  <a href="http://dev.splunk.com/view/event-collector/SP-CAAAE6M">Introduction to Splunk HTTP Event Collector</a>
 *
 */

public class HEC {

    private String channelGUID;
    private String destinationURL;
    private String queryString = "";
    private String token;
    private String source;
    private String sourceType;
    private String index;
    private Logger logger;


    /**
     * Used to construct an HEC object. Implements the Builder pattern.
     */
    public static class HECBuilder {
        private String destinationURL = "";
        private String queryString = "";
        private String token = "";
        private String channelGUID = UUID.randomUUID().toString();
        private String source;
        private String sourceType;
        private String index;
        private Logger logger;


        private void addQueryStringField(String fieldName, String fieldValue) {
            if (StringUtils.isNotEmpty(fieldValue))
                this.queryString +=  (StringUtils.isNotEmpty(this.queryString) ? "&" : "?")
                        + fieldName + "=" + fieldValue;
        }

        /**
         * Add query string to the HEC URL, if override values have been set
         * */
        private void buildQueryString() {
            addQueryStringField("source", this.source);
            addQueryStringField("sourcetype", this.sourceType);
            addQueryStringField("index", this.index);
        }

        /**
         * Set the HEC endpoint - example: http:myservername:myhecport/services/collector/raw/1.0
         * @param destinationURL HEC endpoint URL
         * @return Updated HECBuilder object
         */
        public HECBuilder destinationURL(String destinationURL) {
            this.destinationURL = destinationURL;
            return this;
        }

        /**
         * Override the default "source" metadata
         * @param source New source
         * @return Updated HECBuilder object
         */
        public HECBuilder source(String source) {
            this.source = source;
            return this;
        }

        /**
         * Override "sourcetype" metadata
         * @param sourceType New sourcetype
         * @return Updated HECBuilder object
         */
        public HECBuilder sourceType(String sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        /**
         * Override the default destination Splunk index. Note that your token must be explicitly configured
         * to allow access to this index.
         * @param index Index override
         * @return Updated HECBuilder object
         */
        public HECBuilder index(String index) {
            this.index = index;
            return this;
        }

        /**
         * @param token
         * @return Updated HECBuilder object
         */
        public HECBuilder token(String token) {
            this.token = token;
            return this;
        }

        /**
         *
         * Provide an slf4j logger
         * @param logger org.slf4j.Logger object
         * @return Updated HECBuilder object
         */
        public HECBuilder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * @return  New HEC object
         */
        public HEC build() {
            this.buildQueryString();
            this.destinationURL = this.destinationURL + this.queryString;
            if (this.logger == null) {
                this.logger = LoggerFactory.getLogger(HEC.class);
            }
            return new HEC(this);
        }

    } // HECBuilder

    /**
     * Used to construct an event string using Splunk best practices. Implements the Builder pattern, sort of.
     */
    public static class EventBuilder {
        private final String delim;
        private String event;
        private String timeStamp;

        /**

         * @param delim Character used to delimit field=value pairs.
         *              Example - assuming a delimiter character of '|',
         *              events might look something like:
         *                  ... fieldA="aaaaa123"|fieldB="bbbbb456"|fieldC= ...
         *
         */
        public EventBuilder(String delim) {
            this.delim = delim;
            Calendar calendar = new GregorianCalendar();
            TimeZone tz = calendar.getTimeZone();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");
            df.setTimeZone(tz);
            this.timeStamp = df.format(new Date());
            this.event = this.timeStamp;
        }

        /**
         * Add a single e/value pair to the end of the event string, preceded by
         * the deimiter character.
         * @param fieldName     the key part of the key=value pair
         * @param fieldValue    the value part of the key=value pair
         * @return  Updated EventBuilder object
         */
        public EventBuilder add(String fieldName, String fieldValue) {
            //TODO: fix double-quotes in fieldValue
            this.event = this.event + delim + fieldName + "=\"" + fieldValue + "\"";
            return this;
        }

        /**
         * Add a Map (dictionary) of keys and values to the event.
         * @param eventMap A Map object containing key/value pairs. If field order is important,
         *                 use a LinkedHashMap implementation.
         *
         * @return Updated EventBuilder object
         */
        public EventBuilder add(Map<String, String> eventMap) {
            for (Map.Entry<String, String> entry : eventMap.entrySet()) {
                this.event = this.event + delim + entry.getKey() + "=\"" + entry.getValue() + "\"";
            }
            return this;
        }

        /**
         * @return The contructed event string
         */
        public String build() {
            return event;
        }

    } // EventBuilder


    private HEC(HECBuilder builder) {
        this.destinationURL = builder.destinationURL;
        this.source = builder.source;
        this.sourceType = builder.sourceType;
        this.index = builder.index;
        this.token = builder.token;
        this.channelGUID = builder.channelGUID;
        this.queryString = builder.queryString;
        this.logger = builder.logger;
    }


    /**
     * This method is where the rubber meets the road. It opens a connection to Splunk, sets headers,
     * sends the POST request, reads the response, then disconnects.
     *
     * By default Splunk limits data sent in a single request to 1000000 bytes. Therefore,
     * we'll permit a maximum of 1000000 - 16K bytes (for headers,  etc).
     *
     * @param events String of one or more events
     * @return HTTP response
     * @throws IOException HTTP communications error
     */
    private String sendRawEvents(String events) throws IOException {
        if (events.length() > 983616) {
            logger.error("sendRawEvents received an events string larger than the maximum allowed");
            System.exit(2);
        }

        URL url = new URL(destinationURL);

        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Authorization", "Splunk " + token);
        con.setRequestProperty("x-splunk-request-channel", channelGUID);

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(events.getBytes())));
        DataOutputStream out = new DataOutputStream(con.getOutputStream());
        logger.debug("sendRawEvents() URL=" + destinationURL);

        String event;
        while ((event = br.readLine()) != null) {
            out.writeBytes(event + "\n");
            out.flush();
        }

        out.close();
        String response = con.getResponseMessage();
        con.disconnect();
        logger.debug("sendRawEvents() - response: " + response);
        return response;
    }


    /**
     * Send one or more events stored in a single String Object
     * @param events One or more events, delimited with linefeeds
     * @return HTTP response
     * @throws IOException HTTP communications error
     */
    public String sendMultipleEvents(String events) throws IOException {
        return sendRawEvents(events);
    }

    /**
     * Send one or more events stored in a character array
     * @param events One or more events delimited with linefeeds
     * @return  HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendMultipleEvents(char[] events) throws IOException {
        return sendMultipleEvents(new String(events));
    }

    /**
     * Send one or more events stored in a String array
     * @param events
     * @return  HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendMultipleEvents(String[] events) throws IOException {
        String s = "";
        for (int i=0;i<events.length; i++)
            s += (events[i] + "\n");

        return sendMultipleEvents(s);
    }


    /**
     * Send one or more events stored in a flat file.
     * @param path The full path to a file containing one or more events
     * @return HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendFileOfEvents(String path) throws IOException {
        int maxBlockSizeBytes = 524288; // 512K=524288; 256K=262144; 128K=131072; 64K=65536; 32K=32768; 16K=16384

        char[] buffer = {};
        char[] partial = {};
        int bytesSent = 0;
        int totBytesRead = 0;
        int calls = 0;
        String response = "";

        FileReader f = new FileReader(new File(path));

        for (;;) {

            char[] events;
            buffer = new char[maxBlockSizeBytes];

            int bytesRead = f.read(buffer, 0, maxBlockSizeBytes);
            totBytesRead += bytesRead;
            logger.debug("bytesRead=" + bytesRead + "; buffer length=" + buffer.length
                    + "; partial length=" + partial.length);
            if (bytesRead <= 0)
                break;

            int lastByte = buffer.length;
            if (lastByte == maxBlockSizeBytes) {
                int lastLF = ArrayUtils.lastIndexOf(buffer, '\n');
                if (lastLF > 0)
                    lastByte = lastLF + 1;
            }

            events = ArrayUtils.addAll(partial, ArrayUtils.subarray(buffer, 0, lastByte));
            if (lastByte < buffer.length)
                partial = ArrayUtils.subarray(buffer, lastByte, buffer.length);
            else
                partial = new char[] {};

            logger.trace("Sending " + events.length + " bytes; last char="
                    + Character.getNumericValue(events[events.length-1]));
            response = sendMultipleEvents(events);
            bytesSent += events.length;
            calls++;

            if (bytesRead < maxBlockSizeBytes)
                break;

        }

        logger.debug(totBytesRead + " bytes read; " + bytesSent + " bytes sent; " + calls + " calls");

        f.close();
        return response;

    } // sendFileOfEvents


    /**
     * Send a single event expressed as a Map of field name/value pairs. Use a LinkedHashMap
     * if field order is important.
     * @param eventMap Map of field name/value pairs
     * @return HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendEvent(Map<String, String> eventMap) throws IOException {
        return sendRawEvents((new EventBuilder("|").add(eventMap).build()));
    }


    /**
     * Send a single event to Splunk
     * @param event A single event
     * @return HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendEvent(String event) throws IOException {
        return sendRawEvents(event);
    }


    /**
     * Send one or more events, thenm wait for acknowledgement that the events
     * have been successfully indexed
     * @param events One or more events delimited with linefeeds
     * @return HTTP response
     * @throws IOException Propogated HTTP communications error
     */
    public String sendRawEventWithAck(String events) throws IOException {
        //TODO: finish this
        return(sendRawEvents(events));
    }


    /**
     * Produce a String representation of the HEC object
     * @return String representation of the HEC object
     */
    public String toString() {
        return "source="+source
                +"; sourceType="+sourceType
                +"; index="+index
                +"; token="+token
                +"; channelGUID="+channelGUID
                +"; destinationURL="+destinationURL
                +"; queryString="+queryString;
    }

}
