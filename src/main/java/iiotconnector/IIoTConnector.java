package iiotconnector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriverManager;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class IIoTConnector {

	private static final Logger log = LoggerFactory.getLogger(IIoTConnector.class);

	@Autowired
	private MessageChannel mqttChannel;

	@Value("${plc.host}")
	private String plcHost;

	@Scheduled(fixedRate = 5000)
	public void reportPLCData() {

		String connectionString = "modbus-tcp:tcp://127.0.0.1:10502";
		try {

			var plcConnection = PlcDriverManager.getDefault().getConnectionManager().getConnection(connectionString);

			if (!plcConnection.getMetadata().isReadSupported()) {
				log.error("Reading data from PLC is not supported");
				return;
			}

			// Create a new read request:
			// - Give the single item requested an alias name
			PlcReadRequest.Builder builder = plcConnection.readRequestBuilder();
			builder.addTagAddress("value-1", "holding-register:1");
			builder.addTagAddress("value-2", "holding-register:2");
			builder.addTagAddress("value-3", "holding-register:3");
			builder.addTagAddress("value-4", "holding-register:4");


			PlcReadRequest readRequest = builder.build();

			PlcReadResponse response = readRequest.execute().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS);
			
			PLCDataModel data = new PLCDataModel(response.getInteger("value-1"), response.getInteger("value-2"), response.getInteger("value-3"));
	
			ObjectMapper mapper = new ObjectMapper();
		
			String jsonString = null;
		
			jsonString = mapper.writeValueAsString(data);
			
			log.info(jsonString);
			
			mqttChannel.send(MessageBuilder.withPayload(jsonString).build());

		} catch (PlcConnectionException e) {
			log.error("Error connecting to PLC: " + e.getMessage());			
		} catch (JsonProcessingException e) {
			log.error("Error converting data to JSON: " + e.getMessage());
			throw new RuntimeException(e);
		} catch (Exception e) {
			log.error("Error reading data from PLC: " + e);
		}
	}
}

