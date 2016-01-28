package org.insight_centre.aceis.io.streams.csparql.rabbitmq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.lang3.StringEscapeUtils;
//import org.apache.jena.riot.Lang;
//import org.apache.jena.riot.RDFDataMgr;
//import org.apache.jena.riot.Lang;
//import org.apache.jena.riot.RDFDataMgr;

public class CSPARQLAarhusRabbitMQStream extends RdfStream implements Consumer {
	private static final Logger logger = LoggerFactory.getLogger(CSPARQLAarhusRabbitMQStream.class);

	private Channel channel;
	private String exchange, topic;
	private String queueName;
	private EventDeclaration ed;

	public CSPARQLAarhusRabbitMQStream(String uri, Channel channel, String queueName, String exchange, String topic)
			throws IOException {
		super(uri);
		this.exchange = exchange;
		this.topic = topic;
		this.queueName = queueName;
		this.channel = channel;
		channel.queueBind(queueName, exchange, topic);
		// TODO Auto-generated constructor stub
	}

	public CSPARQLAarhusRabbitMQStream(String uri, Channel channel, String queueName, EventDeclaration ed)
			throws IOException {
		// super(context, uri);
		super(uri);
		this.ed = ed;
		this.exchange = ed.getMsgBusGrounding().getExchange();
		this.topic = ed.getMsgBusGrounding().getTopic();
		// this.topic =
		// ed.getMsgBusGrounding().getTopic().split("/")[ed.getMsgBusGrounding().getTopic().split("/").length - 1];
		// this.topic = "SensorID-1cc03bba-e24c-44bf-b28c-78a1f1e8e26c";
		// this.exchange = "Aarhus_Parking";
		this.queueName = queueName;
		this.channel = channel;
		channel.queueBind(queueName, exchange, topic);
		logger.info("binging queue, exchange: " + exchange + ", topic: " + topic);
	}

	@Override
	public void handleCancel(String arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleCancelOk(String arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleConsumeOk(String arg0) {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {

	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			throws IOException {
		long deliveryTag = envelope.getDeliveryTag();
		String msgBody = new String(body);
		logger.info("Message received: " + this.topic + ", " + msgBody.length());

		channel.basicAck(deliveryTag, false);
		Model m = ModelFactory.createDefaultModel();
		m.read(new ByteArrayInputStream(msgBody.getBytes()), null, "N3");
		List<Statement> stmts = m.listStatements().toList();

		logger.info(this.getIRI() + " Statements received: " + stmts.size());
		for (Statement stmt : stmts) {
			final RdfQuadruple q = new RdfQuadruple(stmt.getSubject().toString(), stmt.getPredicate().toString(), stmt
					.getObject().toString(), System.currentTimeMillis());
			this.put(q);
			// logger.info(this.getIRI() + " Streaming: " + q.toString());
		}
	}

	@Override
	public void handleRecoverOk(String arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleShutdownSignal(String arg0, ShutdownSignalException arg1) {
		// TODO Auto-generated method stub

	}

	public void stop() throws IOException {
		this.channel.basicCancel(this.getIRI());
	}

}
