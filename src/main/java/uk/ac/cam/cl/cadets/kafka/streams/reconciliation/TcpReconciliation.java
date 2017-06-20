/*-
* Copyright (c) 2017 (Graeme Jenkinson)
* All rights reserved.
*
* This software was developed by BAE Systems, the University of Cambridge
* Computer Laboratory, and Memorial University under DARPA/AFRL contract
* FA8650-15-C-7558 ("CADETS"), as part of the DARPA Transparent Computing
* (TC) research program.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
* 1. Redistributions of source code must retain the above copyright
* notice, this list of conditions and the following disclaimer.
* 2. Redistributions in binary form must reproduce the above copyright
* notice, this list of conditions and the following disclaimer in the
* documentation and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
* ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
* FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
* OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
* LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
* OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
* SUCH DAMAGE.
*
*/

package uk.ac.cam.cl.cadets.kafka.streams.reconciliation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * TcpReconiliation correlates aue_accept and aue_connect records based on
 * the same 4-tuple occurring within a defined time window.
 *
 * 4-tuples are extracted from fbt:kernel:cc_conn_init messagaes and are key'd
 * against the correspending socket's so_uuid. This stream is then joined 
 * against the stream of audit::aue-connect (which has been key'd against the
 * corresponding socket's so_uuid).
 */
final public class TcpReconciliation implements Daemon {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(TcpReconciliation.class);

    public static final String PROP_FILENAME = "config.properties";

    public static final String APPLICATION_ID = "application-id";
    public static final String APPLICATION_ID_DEFAULT = "tcp-reconciliation";

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap-servers";
    public static final String BOOTSTRAP_SERVERS_CONFIG_DEFAULT =
        "localhost:9092";

    public static final String METHOD = "reconciliation-method";
    public static final String METHOD_DEFAULT = "distributed-dtrace-4tuple";

    public static final String KAFKA_TOPIC_IN = "kafka-topic-in";
    public static final String KAFKA_TOPIC_IN_DEFAULT = "topic-in";

    public static final String KAFKA_TOPIC_OUT = "kafka-topic-out";
    public static final String KAFKA_TOPIC_OUT_DEFAULT = "topic_out";
        
    public static final String JOINWINDOWS_SIZE_MS = "joinwindows-size-ms";
    public static final String JOINWINDOWS_SIZE_MS_DEFAULT =
        Long.toString(TimeUnit.MINUTES.toMillis(500));

    public static final String JOINWINDOWS_RETENTION_SIZE_MS =
        "joinwindows-retention-size-ms";
    public static final String JOINWINDOWS_RETENTION_SIZE_MS_DEFAULT =
        Long.toString(TimeUnit.MINUTES.toMillis(10000));

    public static final UUID distributedDtraceUuid =
            UUID.fromString("00000000-0000-0000-0000-000000000001");

    private KafkaStreams streams;
    
    @Override
    public void init(final DaemonContext daemonContext)
        throws DaemonInitException, Exception {
	LOGGER.info("Initialising TcpReconciliation");

        _init();
    }

    @Override
    public void start() throws Exception {
	LOGGER.info("Starting TcpReconciliation");
        streams.start();
    
        // Add shutdown hook to respond to SIGTERM and gracefully close
        // Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @Override
    public void stop() throws Exception {
	LOGGER.info("Stopping TcpReconciliation");
        streams.close();
    }

    @Override
    public void destroy() {
	LOGGER.info("Destroying TcpReconciliation");
        streams.close();
    }

    private void _init() throws FileNotFoundException {

        // Load the configuration properties from a file 
        final Properties configProperties = new Properties();
        try (final InputStream in = TcpReconciliation.class
            .getClassLoader().getResourceAsStream(PROP_FILENAME)) {
            configProperties.load(in);
        } catch (IOException e) {
            throw new FileNotFoundException("config.properties not found");
        }

        // Display the configuration properties
        LOGGER.info("TcpReconciliation config: ");

        LOGGER.info("TcpReconciliation UUID = {}", distributedDtraceUuid);

        final long joinWindowSize = Integer.parseInt(
                configProperties.getProperty(
                JOINWINDOWS_SIZE_MS, JOINWINDOWS_SIZE_MS_DEFAULT));
        LOGGER.info("{} = {} ms", JOINWINDOWS_SIZE_MS, joinWindowSize);
    
        final long windowRetentionSize = Integer.parseInt(
                configProperties.getProperty(
                JOINWINDOWS_RETENTION_SIZE_MS,
                JOINWINDOWS_RETENTION_SIZE_MS_DEFAULT));
        LOGGER.info("{} = {} ms",
            JOINWINDOWS_RETENTION_SIZE_MS, windowRetentionSize); 
            
        final String topicIn = configProperties.getProperty(
                KAFKA_TOPIC_IN, KAFKA_TOPIC_IN_DEFAULT);
        LOGGER.info("{} = {}", KAFKA_TOPIC_IN, topicIn);

        final String topicOut = configProperties.getProperty(
                KAFKA_TOPIC_OUT, KAFKA_TOPIC_OUT_DEFAULT);
        LOGGER.info("{} = {}", KAFKA_TOPIC_OUT, topicOut);

        final String method =
            configProperties.getProperty(METHOD, METHOD_DEFAULT);
        LOGGER.info("{} = {}", METHOD, method);

	final Serializer<JsonNode> jsonSerializer = new JsonSerializer();

	final Deserializer<JsonNode> jsonDeserializer =
            new JsonWithEmptyDeser();

        final Serde<JsonNode> jsonSerde =
            Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Serde<TcpReconciliationRecord> tcpReconciliationRecordSerde =
            Serdes.serdeFrom(new TcpReconciliationRecordSerializer(),
               new TcpReconciliationRecordDeserializer());

        final Serde<String> stringSerde = Serdes.String();

        final Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
            configProperties.getProperty(
                APPLICATION_ID, APPLICATION_ID_DEFAULT));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            configProperties.getProperty(
                BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS_CONFIG_DEFAULT));
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
		    Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
	    TimestampExtractor.class); 
        // setting offset reset to earliest so that we can re-run the demo code
        // with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KStreamBuilder builder = new KStreamBuilder();

        // Construct a Kafka Stream from the Kafak topic specified by the
        // configuration parameter KAFKA_TOPIC_IN, this trace is send
        // unchanged to the Kafak topic specified by the configuration
        // parameter KAFKA_TOPIC_OUT
        final KStream<String, JsonNode> cadetsTrace =
            builder.stream(stringSerde, jsonSerde, topicIn)
            // Filter events that either aren't valid JSON (null), or
            // do not contain the "event" and "host" fields
            .filterNot((k, v) -> v == null ||
                v.get("event").isNull() ||
                v.get("host").isNull())
            // Append the host uuid to each field of the event that matches
            // *uuid
            .mapValues(v -> {
                final String hostuuid = v.get("host").textValue();
                final Iterator<Map.Entry<String, JsonNode>> nodes = v.fields();
                while (nodes.hasNext()) {
                    final Map.Entry<String, JsonNode> entry =
                        (Map.Entry<String, JsonNode>) nodes.next(); 
                    final String key = entry.getKey();
                    if (key.contains("uuid")) {
                       final String value = entry.getValue().textValue();
                       ((ObjectNode) v).put(key, hostuuid + ":" + value);
                    }
                }
                return v;
            })
            .through(stringSerde, jsonSerde, topicOut);

        // Split the input stream into braches containing TCP 4-tuples and
        // aue_accept and aue_connect events
        final Predicate<String, JsonNode> isConnect =
            (k, v) -> v.get("event").textValue()
                .equals("audit:event:aue_connect:");

        final Predicate<String, JsonNode> isCcConnInit =
            (k, v) -> v.get("event").textValue()
                .equals("fbt:kernel:cc_conn_init:");

        final Predicate<String, JsonNode> isAccept =
            (k, v) -> v.get("event").textValue()
                .equals("audit:event:aue_accept:");

        final Predicate<String, JsonNode> isSyncacheExpand =
            (k, v) -> v.get("event").textValue()
                .equals("fbt:kernel:syncache_expand:");

        final KStream<String, JsonNode> filteredTraces [] =
            cadetsTrace.branch(isConnect, isCcConnInit,
            isAccept, isSyncacheExpand);
 
        // Join the connect and ccConnInit traces togther having first
        // key'd them on the socket's uuid

	final KStream<String, JsonNode> connectTrace =
	    filteredTraces[0]
            .filterNot((k, v) -> v.get("arg_objuuid1").isNull())
            // Key record on the value of arg_objuuid1
            .selectKey((k, v) -> v.get("arg_objuuid1").textValue());

        final KStream<String, JsonNode> ccConnInitTrace =
	    filteredTraces[1]
            .filterNot((k, v) -> v.get("so_uuid").isNull())
            // Key record on so_uuid
            .selectKey((k, v) -> v.get("so_uuid").textValue());

	final KStream<String, JsonNode> joinedConnectTrace =
            connectTrace.join(ccConnInitTrace,
                (left, right) -> {
                    ((ObjectNode) left).put("lport",
                        right.get("lport").intValue());
                    ((ObjectNode) left).put("fport",
                        right.get("fport").intValue());
                    ((ObjectNode) left).put("laddr",
                        right.get("laddr").textValue());
                    ((ObjectNode) left).put("faddr",
                        right.get("faddr").textValue());
                    return left;
                },
                JoinWindows.of(joinWindowSize).until(windowRetentionSize),
                stringSerde,
                jsonSerde,
                jsonSerde)
            .selectKey((k, v) -> 
                Integer.toString(v.get("lport").intValue()) +
                ":" + Integer.toString(v.get("fport").intValue()) +
                ":" + v.get("laddr").textValue() +
                ":" + v.get("faddr").textValue()
                );

        // Join the accept and syncacheExpand traces togther having first
        // key'd them on the socket's uuid

	final KStream<String, JsonNode> acceptTrace =
	    filteredTraces[2]
            .filterNot((k, v) -> v.get("arg_objuuid1").isNull())
            // Key record on the value of arg_objuuid1
            .selectKey((k, v) -> v.get("arg_objuuid1").textValue());

 	final KStream<String, JsonNode> syncacheExpandTrace =
	    filteredTraces[3]
            .filterNot((k, v) -> v.get("so_uuid").isNull())
            // Key record on so_uuid
            .selectKey((k, v) -> v.get("so_uuid").textValue());

	final KStream<String, JsonNode> joinedAcceptTrace =
            acceptTrace.join(syncacheExpandTrace,
                (left, right) -> {
                    ((ObjectNode) left).put("lport",
                        right.get("fport").intValue());
                    ((ObjectNode) left).put("fport",
                        right.get("lport").intValue());
                    ((ObjectNode) left).put("laddr",
                        right.get("faddr").textValue());
                    ((ObjectNode) left).put("faddr",
                        right.get("laddr").textValue());
                    return left;
                },
                JoinWindows.of(joinWindowSize).until(windowRetentionSize),
                stringSerde,
                jsonSerde,
                jsonSerde)
            .selectKey((k, v) -> 
                Integer.toString(v.get("lport").intValue()) +
                ":" + Integer.toString(v.get("fport").intValue()) +
                ":" + v.get("laddr").textValue() +
                ":" + v.get("faddr").textValue()
                );

        final KStream<String, JsonNode> tcpReconciliation =
            joinedConnectTrace.join(joinedAcceptTrace,
            (leftValue, rightValue) -> new TcpReconciliationRecord(
                leftValue.get("arg_objuuid1").textValue(),
                rightValue.get("ret_objuuid1").textValue(),
                method,
                distributedDtraceUuid,
                0.0F).toJsonNode(),
            JoinWindows.of(joinWindowSize).until(windowRetentionSize),
            stringSerde,
            jsonSerde,
            jsonSerde);

        tcpReconciliation.print();

        // Send the TCP reconciliation stream to the output Kafka topic
        tcpReconciliation.to(stringSerde, jsonSerde, topicOut);

        streams = new KafkaStreams(builder, props);
    }

    public static void main(final String[] args) throws Exception {

        final TcpReconciliation tcpRecon = new TcpReconciliation();
        tcpRecon._init();
        tcpRecon.start();
    }
}
