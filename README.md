# datafusion_cdap_alert_publisher_plugin
Complete example project to create a custom Google cloud datafusion (CDAP) alert publisher plugin. Sourced and adapted from the documentation where there is no quickstart project.

## Alert Publisher Plugin
An AlertPublisher plugin is a special type of plugin that consumes alerts emitted from previous stages instead of output records. Alerts are meant to be uncommon events that need to be acted on in some other program. Alerts contain a payload, which is just a map of strings containing any relevant data. An alert publisher is responsible for writing the alerts to some system, where it can be read and acted upon by some external program. For example, a plugin may write alerts to Kafka. Alerts may not be published immediately after they are emitted. It is up to the processing engine to decide when to publish alerts.

The only method that needs to be implemented is: publish(Iterator<Alert> alerts)

## Methods
### initialize(): 
Used to perform any initialization step that might be required during the runtime of the AlertPublisher. It is guaranteed that this method will be invoked before the publish method.
### publish():
This method contains the logic that will publish each incoming Alert.
### destroy():
Used to perform any cleanup before the plugin shuts down.