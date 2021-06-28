package com.murali.kafka.connectors;

import com.google.common.base.Joiner;
import com.murali.kafka.connectors.utils.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class MqttSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnector.class);
    private MqttSourceConnectorConfig mqttSourceConnectorConfig;
    private Map<String, String> config;


    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> configValues) {
        log.info("INFO : Starting Source Connector");
        config = configValues;
        mqttSourceConnectorConfig = new MqttSourceConnectorConfig(configValues);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> result = new ArrayList<>();
        Iterator iterator = ConnectorUtils.groupPartitions(Arrays.asList(this.mqttSourceConnectorConfig.mqttTopic), maxTasks).iterator();
        while (iterator.hasNext()) {
            List<String> mqttTopics = (List) iterator.next();
            if (!mqttTopics.isEmpty()) {
                Map<String, String> settings = new LinkedHashMap<>(this.config);
                settings.put("mqtt.topics", Joiner.on(',').join(mqttTopics));
                result.add(settings);
            }
        }
        return result;
    /*    log.info("INFO : Max tasks defined {}", maxTasks);
        List<Map<String, String>> result = new ArrayList<Map<String, String>>(1);
        result.add(new LinkedHashMap<String, String>(this.config));
        log.info("INFO : Setting default task to 1");
        return result;
*/

    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.log
        log.info("Stopping Source Connector");
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConnectorConfig.config();
    }
}
