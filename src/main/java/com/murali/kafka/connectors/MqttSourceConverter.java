package com.murali.kafka.connectors;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MqttSourceConverter {
    private static Logger log = LoggerFactory.getLogger(MqttSourceConverter.class);

    static final Map<String, Object> EMPTY_MAP = ImmutableMap.of();
    static final String HEADER_PREFIX = "mqtt.";
    static final String HEADER_MESSAGE_ID = "mqtt.message.id";
    static final String HEADER_QOS = "mqtt.qos";
    static final String HEADER_RETAINED = "mqtt.retained";
    static final String HEADER_DUPLICATE = "mqtt.duplicate";
    private final MqttSourceConnectorConfig config;
    private final Time time;

    MqttSourceConverter(MqttSourceConnectorConfig config, Time time) {
        this.config = config;
        this.time = time;
    }

    public SourceRecord convert(String mqttTopic, MqttMessage message) {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("mqtt.message.id", message.getId());
        headers.addInt("mqtt.qos", message.getQos());
        headers.addBoolean("mqtt.retained", message.isRetained());
        headers.addBoolean("mqtt.duplicate", message.isDuplicate());
        log.info("LOG INFO : Building New Source Record.Incoming Message'{}'", new String(message.getPayload()));
        SourceRecord result = new SourceRecord(
                EMPTY_MAP, EMPTY_MAP,
                this.config.kafkaTopic,
                (Integer) null,
                Schema.STRING_SCHEMA,
                mqttTopic,
                Schema.STRING_SCHEMA,
                new String(message.getPayload()),
                this.time.milliseconds(),
                headers);
        return result;
    }
}