package com.alecuba16.cdap;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Alert Publisher Plugin
 * An AlertPublisher plugin is a special type of plugin that consumes alerts emitted from previous stages instead of
 * output records. Alerts are meant to be uncommon events that need to be acted on in some other program. Alerts contain
 * a payload, which is just a map of strings containing any relevant data. An alert publisher is responsible for writing
 * the alerts to some system, where it can be read and acted upon by some external program. For example, a plugin may
 * write alerts to Kafka. Alerts may not be published immediately after they are emitted. It is up to the processing
 * engine to decide when to publish alerts.

 * The only method that needs to be implemented is: publish(Iterator<Alert> alerts)

 * Methods
 * initialize(): Used to perform any initialization step that might be required during the runtime of the
 * AlertPublisher. It is guaranteed that this method will be invoked before the publish method.
 * publish(): This method contains the logic that will publish each incoming Alert.
 * destroy(): Used to perform any cleanup before the plugin shuts down.
 */
/**
 * Publishes alerts to TMS.
 */
@Plugin(type = AlertPublisher.PLUGIN_TYPE)
@Name("TMS")
@Description("Publishes alerts to the CDAP Transaction Messaging System. Alerts will be formatted as json objects.")
public class TMSAlertPublisher extends AlertPublisher {
    public static final Gson GSON = new Gson();
    private static final Logger LOG = LoggerFactory.getLogger(TMSAlertPublisher.class);
    private final Conf conf;
    private MessagePublisher messagePublisher;
    private String publishNamespace;

    public TMSAlertPublisher(Conf conf) {
        this.conf = conf;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        conf.validate();
    }

    @Override
    public void initialize(AlertPublisherContext context) throws Exception {
        super.initialize(context);
        try {
            context.getTopicProperties(conf.topic);
        } catch (TopicNotFoundException e) {
            if (conf.autoCreateTopic) {
                // this is checked at configure time unless namespace is a macro
                if (conf.namespace != null) {
                    throw new IllegalArgumentException(
                            String.format("Topic '%s' does not exist and cannot be auto-created since namespace is set." +
                                    "Topics can only be auto-created if no namespace is given.", conf.topic));
                }

                try {
                    context.createTopic(conf.topic);
                } catch (TopicAlreadyExistsException e1) {
                    // somebody happened to create it at the same time, ignore
                }
            } else {
                throw e;
            }
        }
        messagePublisher = context.getDirectMessagePublisher();
        // TODO: use pipeline namespace instead of 'default' once namespace is available through context
        publishNamespace = conf.namespace == null ? context.getNamespace() : conf.namespace;
    }

    @Override
    public void publish(Iterator<Alert> iterator) throws Exception {
        long tickTime = System.currentTimeMillis();
        int publishedSinceLastTick = 0;
        while (iterator.hasNext()) {
            messagePublisher.publish(publishNamespace, conf.topic, GSON.toJson(iterator.next()));
            publishedSinceLastTick++;
            long currentTime = System.currentTimeMillis();
            if (currentTime - tickTime > 1000) {
                tickTime = currentTime;
                publishedSinceLastTick = 0;
            } else if (publishedSinceLastTick >= conf.maxAlertsPerSecond) {
                long sleepTime = tickTime + 1000 - currentTime;
                LOG.info("Hit maximum of {} published alerts in the past second, sleeping for {} millis.",
                        publishedSinceLastTick, sleepTime);
                TimeUnit.MILLISECONDS.sleep(tickTime + 1000 - currentTime);
            }
        }
    }

    /**
     * Plugin configuration
     */
    public static class Conf extends PluginConfig {
        @Macro
        @Description("The TMS topic to publish messages to.")
        private String topic;

        @Macro
        @Nullable
        @Description("The namespace of the topic to publish messages to. If none is specified, " +
                "the pipeline namespace will be used.")
        private String namespace;

        @Nullable
        @Description("Whether to create the topic in the pipeline namespace if the topic does not already exist. " +
                "Cannot be set to true if namespace is set. Defaults to false.")
        private Boolean autoCreateTopic;

        @Nullable
        @Description("The maximum number of alerts to publish per second. Defaults to 100.")
        private Integer maxAlertsPerSecond;

        private Conf() {
            topic = null;
            namespace = null;
            autoCreateTopic = false;
            maxAlertsPerSecond = 100;
        }

        private void validate() {
            if (autoCreateTopic && namespace != null) {
                throw new IllegalArgumentException("Cannot auto-create topic when namespace is set.");
            }
            if (maxAlertsPerSecond < 1) {
                throw new IllegalArgumentException(
                        String.format("Invalid maxAlertsPerSecond %d. Must be at least 1.", maxAlertsPerSecond));
            }
        }
    }
}