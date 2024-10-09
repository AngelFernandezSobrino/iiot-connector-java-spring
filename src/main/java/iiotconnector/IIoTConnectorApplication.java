package iiotconnector;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
@EnableIntegration
public class IIoTConnectorApplication {

	public static void main(String[] args) {
		
		ConfigurableApplicationContext context = new SpringApplicationBuilder(IIoTConnectorApplication.class)
			.web(WebApplicationType.NONE)
			.run(args);
	}

	@Value("${mqtt.broker.url}")
	private String brokerUrlString;

	@Bean
    MqttPahoClientFactory mqttPahoClientFactory() {
        var factory = new DefaultMqttPahoClientFactory();
        var options = new MqttConnectOptions();
        options.setServerURIs(new String[]{brokerUrlString});
        factory.setConnectionOptions(options);
        return factory;
    }

	@Bean
    MessageChannel mqttChannel() {
        return new DirectChannel();
    }

    @Bean
    MqttPahoMessageHandler mqttOutboundAdapter(
            @Value("${mqtt.topic}") String topic,
            MqttPahoClientFactory factory) {
        var mh = new MqttPahoMessageHandler("plcProducer", factory);
        mh.setDefaultTopic(topic);
        return mh;
    }

    @Bean
    IntegrationFlow outboundFlow(MessageChannel mqttChannel,
                                 MqttPahoMessageHandler mqttOutBoundAdapter) {
        return IntegrationFlow
                .from(mqttChannel)
                .handle(mqttOutBoundAdapter)
                .get();
    }

}
