package com.murali.kafka.connectors;

import com.murali.kafka.connectors.builders.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class MqttSourceConnectorConfig extends AbstractConfig {

    public static final String MQTT_TOPIC = "mqtt.topics";
    private static final String MQTT_TOPIC_DOC = "Name of the Mqtt topic(s) to subscribe with";

    public static final String KAFKA_TOPIC = "kafka.topic";
    private static final String KAFKA_TOPIC_DOC = "Name of the kafka topic to write the data to";

    public static final String MQTT_SERVER_URI = "mqtt.server.uri";
    private static final String MQTT_SERVER_URI_DOC = "mqtt server uri address";

    public static final String MQTT_QOS = "mqtt.service.quality";
    private static final String MQTT_QOS_DOC = "The MQTT QOS level to subscribe with.";

    public final String mqttTopic;
    public final String kafkaTopic;
    public final String mqttServerUri;
    public final Integer mqttServiceQuality;


    public MqttSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.mqttTopic=this.getString(MQTT_TOPIC);
        this.kafkaTopic=this.getString(KAFKA_TOPIC);
        this.mqttServerUri=this.getString(MQTT_SERVER_URI);
        this.mqttServiceQuality=this.getInt(MQTT_QOS);
    }


    public static ConfigDef config() {
        final ConfigDef define = new ConfigDef().define(
                ConfigKeyBuilder.of(MQTT_TOPIC, Type.STRING)
                        .documentation(MQTT_TOPIC_DOC)
                        .importance(Importance.HIGH)
                        .build())
                .define(
                        ConfigKeyBuilder.of(KAFKA_TOPIC, Type.STRING)
                                .documentation(KAFKA_TOPIC_DOC)
                                .importance(Importance.HIGH)
                                .build())
                .define(
                        ConfigKeyBuilder.of(MQTT_SERVER_URI, Type.STRING)
                                .documentation(MQTT_SERVER_URI_DOC)
                                .importance(Importance.HIGH)
                                .build())
                .define(
                        ConfigKeyBuilder.of(MQTT_QOS, Type.INT)
                                .documentation(MQTT_QOS_DOC)
                                .importance(Importance.LOW)
                                .defaultValue(0)
                                .validator(ConfigDef.Range.between(0, 2))
                                .build()
                );
        return define;
    }

}
