package org.insight_centre.citypulse.main;

import org.insight_centre.aceis.engine.ACEISEngine;
import org.insight_centre.aceis.engine.ACEISEngine.RspEngine;
import org.insight_centre.aceis.engine.ACEISFactory;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.streams.csparql.rabbitmq.CSPARQLAarhusRabbitMQStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MessagingTest {
	private static final Logger logger = LoggerFactory.getLogger(MessagingTest.class);

	public static void main(String[] args) {
		try {
			ACEISEngine engine = ACEISFactory.createACEISInstance(RspEngine.CSPARQL, null);
			engine.initialize("Scenario2Sensors.n3");
			// ExecContext context = new ExecContext(RDFFileManager.cqelsHome, true);
			// String exchange = "traffic";
			// String topic = "185396";
			ConnectionFactory factory = new ConnectionFactory();
			String serverAddr = "amqp://guest:guest@131.227.92.55:8007";
			logger.info("server address: " + serverAddr);
			Channel channel = null;
			// for (Channel c : this.msgBusChannels) {
			// if (c.getConnection().getAddress().toString().equals(serverAddr))
			// channel = c;
			// }
			if (channel == null) {
				// if (this.msgBusChannels.contains(serverAddr))
				factory.setUri(serverAddr);
				Connection conn = factory.newConnection();
				channel = conn.createChannel();
			}

			String exchange = "Aarhus_Traffic";
			// if (exchange.toLowerCase().contains("traffic"))
			// exchange = "Aarhus_Traffic";
			// exchange = "Aarhus_Parking";
			channel.exchangeDeclare(exchange, "direct");
			logger.info("Declaraing exchange: " + exchange);
			String queueName = channel.queueDeclare().getQueue();
			// String topic = ed.getMsgBusGrounding().getTopic();
			logger.info("channel opened.");
			String serviceId = "http://ict-citypulse.eu/PrimitiveEventService-1cc03bba-e24c-44bf-b28c-78a1f1e8e26c";
			EventDeclaration ed = engine.getRepo().getEds().get(serviceId);
			CSPARQLAarhusRabbitMQStream stream = new CSPARQLAarhusRabbitMQStream("", channel, queueName, ed);
			channel.basicConsume(queueName, false, stream.getIRI(), stream);
			engine.getCsparqlEngine().registerStream(stream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
