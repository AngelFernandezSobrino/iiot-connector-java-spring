package iiotconnector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.digitalpetri.modbus.master.ModbusTcpMaster;
import com.digitalpetri.modbus.master.ModbusTcpMasterConfig;
import com.digitalpetri.modbus.requests.ReadHoldingRegistersRequest;
import com.digitalpetri.modbus.responses.ModbusResponse;

import iiotconnector.models.PLCDataModel;


@Component
public class IIoTConnector {

	private static final Logger log = LoggerFactory.getLogger(IIoTConnector.class);

	@Autowired
	private MessageChannel mqttChannel;

	@Value("${plc.host}")
	private String plcHost;

	@Scheduled(fixedRate = 5000)
	public void reportPLCData() {

		ModbusTcpMasterConfig config = new ModbusTcpMasterConfig.Builder(plcHost)
			.setPort(10502)
			.build();		
			
		ModbusTcpMaster client = new ModbusTcpMaster(config);
			
		
		try {
			client.connect();
	
			CompletableFuture<ModbusResponse> responseFuture = client.sendRequest(new ReadHoldingRegistersRequest(0, 10), 0);
			
			ModbusResponse result = responseFuture.get();
		
			log.info("Response: " + result);
	
			client.disconnect();
	
			PLCDataModel data = new PLCDataModel(0, 0, 0);
	
			ObjectMapper mapper = new ObjectMapper();
		
			String jsonString = null;
		
			jsonString = mapper.writeValueAsString(data);
			
			log.info(jsonString);
			
			mqttChannel.send(MessageBuilder.withPayload(jsonString).build());
		
		} catch (InterruptedException | ExecutionException e) {
			log.error("Error reading from PLC: " + e.getMessage());
			return;
		} catch (JsonProcessingException e) {
			log.error("Error converting data to JSON: " + e.getMessage());
			throw new RuntimeException(e);
		}
			
	}
}

