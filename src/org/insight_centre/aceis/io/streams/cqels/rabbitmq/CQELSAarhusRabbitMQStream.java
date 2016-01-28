package org.insight_centre.aceis.io.streams.cqels.rabbitmq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

//import org.apache.commons.io.IOUtils;
//import org.apache.jena.riot.Lang;
//import org.apache.jena.riot.RDFDataMgr;
//import org.apache.jena.riot.Lang;
//import org.apache.jena.riot.RDFDataMgr;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
//import org.apache.jena.riot.RDFDataMgr;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;

public class CQELSAarhusRabbitMQStream extends RDFStream implements Consumer {
	private Channel channel;
	private String exchange, topic;
	private String queueName;
	private EventDeclaration ed;

	public CQELSAarhusRabbitMQStream(ExecContext context, String uri, Channel channel, String queueName,
			String exchange, String topic) throws IOException {
		super(context, uri);
		this.exchange = exchange;
		this.topic = topic;
		this.queueName = queueName;
		this.channel = channel;
		channel.queueBind(queueName, exchange, topic);

	}

	public CQELSAarhusRabbitMQStream(ExecContext context, Channel channel, String queueName, EventDeclaration ed)
			throws IOException {
		super(context, ed.getServiceId());
		this.ed = ed;
		this.exchange = ed.getMsgBusGrounding().getExchange();
		this.topic = ed.getMsgBusGrounding().getTopic();
		this.queueName = queueName;
		this.channel = channel;
		channel.queueBind(queueName, exchange, topic);

	}

	private static final Logger logger = LoggerFactory.getLogger(CQELSAarhusRabbitMQStream.class);

	// static ConnectionFactory factory = new ConnectionFactory();
	// static Connection conn = null;
	// static Channel channel = null;
	// static String queueName = null;
	// static {
	//
	//
	// }
	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			throws IOException {
		long deliveryTag = envelope.getDeliveryTag();
		String msgBody = new String(body);
		// logger.info("Message received: " + this.topic + " " + msgBody);

		channel.basicAck(deliveryTag, false);
		Model m = ModelFactory.createDefaultModel();
		m.read(new ByteArrayInputStream(msgBody.getBytes()), null, "N3");
		List<Statement> stmts = m.listStatements().toList();
		logger.info(this.getURI() + ": Statements received: " + stmts.size());
		for (Statement stmt : stmts) {
			stream(stmt.getSubject().toString(), stmt.getPredicate().toString(), stmt.getObject().toString());
			logger.debug(this.getURI() + " Streaming: " + stmt.toString());
		}
	}

	public static void main(String[] args) {

		// CQELSAarhusTrafficStreamRabbitMQ stream2 = new CQELSAarhusTrafficStreamRabbitMQ(null, null);
		// new Thread(stream).start();

		try {
			ExecContext context = new ExecContext(RDFFileManager.cqelsHome, true);
			String exchange = "myexchange";
			String topic = "mytopic";
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri("amqp://guest:guest@131.227.92.55:8007");
			Connection conn = factory.newConnection();
			final Channel channel = conn.createChannel();
			channel.exchangeDeclare(exchange, "direct");
			String queueName = channel.queueDeclare().getQueue();
			// channel.queueBind(queueName, exchange, topic);
			logger.info("channel opened.");
			CQELSAarhusRabbitMQStream stream = new CQELSAarhusRabbitMQStream(context, "sampleuri", channel, queueName,
					exchange, topic);
			channel.basicConsume(queueName, false, stream.getURI(), stream);
			// channel.basicPublish(arg0, arg1, arg2, arg3);
			try {
				Thread.sleep(5000);
				channel.basicCancel(stream.getURI());
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// new Thread(stream2).start();
	}

	@Override
	public void stop() {
		try {
			this.channel.basicCancel(this.getURI());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// this.

	}

	@Override
	public void handleCancel(String arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleConsumeOk(String arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRecoverOk(String arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleShutdownSignal(String arg0, ShutdownSignalException arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleCancelOk(String arg0) {
		// TODO Auto-generated method stub

	}

}
