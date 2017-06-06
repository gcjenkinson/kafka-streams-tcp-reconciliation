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

package uk.ac.cam.cl.cadets;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;

import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

final public class TcpReconciliation {

    public static final String APPLICATION_ID = "tcp-reconciliation";

    public static final String TOPIC_IN = "ddtrace-query-response";
    public static final String TOPIC_OUT = "tcp-reconciliation";

    public static void main(String[] args) throws Exception {

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

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "172.16.100.164:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,
            "172.16.100.164:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
		    Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
	    TimestampExtractor.class); 
        // setting offset reset to earliest so that we can re-run the demo code
        // with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStreamBuilder builder = new KStreamBuilder();

        final Predicate<String, JsonNode> isConnect =
            (k, v) -> v.get("event").textValue()
                .equals("audit:event:aue_connect:");

        final Predicate<String, JsonNode> isAccept =
            (k, v) -> v.get("event").textValue()
                .equals("audit:event:aue_accept:");

        final KStream<String, JsonNode> cadetsTrace =
            builder.stream(stringSerde, jsonSerde, TOPIC_IN);

        final KStream<String, JsonNode> filteredTraces [] = cadetsTrace
            .filterNot((k, v) -> v == null || v.get("event").isNull())
            .branch(isConnect, isAccept);
        
	final KStream<String, JsonNode> connectTrace =
	    filteredTraces[0]
            // Key record on TCP 4-tuple
            .selectKey((key, value) -> "172.16.100.97:4501:172.16.172.98:22");
        connectTrace.print();
        
	final KStream<String, JsonNode> acceptTrace =
	    filteredTraces[1]
            // Key record on TCP 4-tuple
            .selectKey((key, value) -> "172.16.100.97:4501:172.16.172.98:22");
        acceptTrace.print();
        
        final long joinWindowSize = TimeUnit.MINUTES.toMillis(20);
        System.out.println(joinWindowSize);
        final long windowRetentionSize = TimeUnit.MINUTES.toMillis(15);
        System.out.println(windowRetentionSize);
         
        final UUID distributedDtraceUuid =
            UUID.fromString("00000000-0000-0000-0000-000000000001");
        System.out.println("distributed-dtrace UUID " + distributedDtraceUuid);

        TcpReconciliationRecord test = new TcpReconciliationRecord(
                "0",
                "1",
                "distribute-dtrace",
                distributedDtraceUuid,
                0.0F);

        final KStream<String, TcpReconciliationRecord> tcpReconciliation =
            connectTrace.join(acceptTrace,
            (leftValue, rightValue) -> new TcpReconciliationRecord(
                leftValue.get("arg_objuuid1").textValue(),
                rightValue.get("arg_objuuid1").textValue(),
                "distribute-dtrace",
                distributedDtraceUuid,
                0.0F),
            JoinWindows.of(joinWindowSize), //.until(windowRetentionSize),
            stringSerde,
            jsonSerde,
            jsonSerde)
            // Send the TCP reconciliation stream to the output Kafka topic
            .through(stringSerde, tcpReconciliationRecordSerde, TOPIC_OUT);
        tcpReconciliation.print();

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close
        // Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
