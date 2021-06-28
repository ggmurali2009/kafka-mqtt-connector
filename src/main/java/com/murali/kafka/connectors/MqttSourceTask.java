package com.murali.kafka.connectors;


import com.murali.kafka.connectors.utils.VersionUtil;
import com.murali.kafka.connectors.utils.data.SourceRecordDeque;
import com.murali.kafka.connectors.utils.data.SourceRecordDequeBuilder;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MqttSourceTask extends SourceTask implements IMqttMessageListener {

    private static final Logger log = LoggerFactory.getLogger(MqttSourceTask.class);
    MqttSourceConnectorConfig config;
    MqttSourceConverter mqttSourceConverter;
    IMqttMessageListener listeners;
    IMqttClient client;
    String subscriberId;
    SourceRecordDeque records;
    Time time;

    /*config details*/
    private String kafkaTopic;
    private String mqttTopic;

    /*Records details*/
    private final int bachSize;
    private final int emptyWaitMs;
    private final int maximumCapacity;
    private final int maximumCapacityTimeoutMs;


    public MqttSourceTask() {
        this.time = Time.SYSTEM;
        this.bachSize = 4096;
        this.emptyWaitMs = 100;
        this.maximumCapacity = 50000;
        maximumCapacityTimeoutMs = 60000;
    }


    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> configValues) {
        this.config = new MqttSourceConnectorConfig(configValues);
        this.mqttSourceConverter = new MqttSourceConverter(this.config, this.time);
        this.records = SourceRecordDequeBuilder.of()
                .batchSize(this.bachSize)
                .emptyWaitMs(this.emptyWaitMs)
                .maximumCapacityTimeoutMs(this.maximumCapacityTimeoutMs)
                .maximumCapacity(this.maximumCapacity)
                .build();
        startMqttClient(this.config);
    }

    public void startMqttClient(MqttSourceConnectorConfig configValues) {
        try {
            this.subscriberId = MqttClient.generateClientId();
            log.info("LOG INFO : Creating New Mqtt client with subcriberid {}", this.subscriberId);
            this.client = new MqttClient(configValues.mqttServerUri, this.subscriberId);
        } catch (MqttException e) {
            throw new ConnectException(e);
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);

        try {
            log.info("Connecting to Mqtt Broker");
            this.client.connect(options);
        } catch (MqttException e) {
            throw new ConnectException(e);
        }

        try {
            log.info("LOG INFO : Subscribing to the topic '{}'.", configValues.mqttTopic);
            this.client.subscribe(configValues.mqttTopic, configValues.mqttServiceQuality, this);
        } catch (MqttException e) {
            log.error("Error While Subscribing to Mqtt Topic '{}'", configValues.mqttTopic);
            e.printStackTrace();
        }

        System.out.println("Subscriber Application end..");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //TODO: Create SourceRecord objects that will be sent the kafka cluster.
        if (!this.client.isConnected()) {
            throw new UnsupportedOperationException("This has not been implemented.");
        }
        // log.info("Polling data-returns SourceRecords");
        List<SourceRecord> sourceRecordList = this.records.getBatch();
        if (sourceRecordList != null) {
            if (!sourceRecordList.isEmpty()) {
                log.info("LOG INTO : Record received from Mqtt Topic '{}'.", sourceRecordList);
            }
        }
        return sourceRecordList;
    }

    @Override
    public void stop() {
        if (this.client.isConnected()) {
            try {
                this.client.disconnect();
            } catch (MqttException exception) {
                log.error("Exception thrown while disconnecting client.", exception);
            }
        }
    }

    @Override
    public void messageArrived(String mqttTopicName, MqttMessage mqttMessage) throws Exception {
        log.info("LOG INFO : Message Arrived");
        log.info("LOG INFO : Incoming message from Mqtt brokers,MqttTopic name:'{}' and MqttMessage :'{}'.", mqttTopicName, mqttMessage);
        SourceRecord record = this.mqttSourceConverter.convert(mqttTopicName, mqttMessage);
        this.records.add(record);
    }
}