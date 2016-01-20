package org.insight_centre.aceis.subscriptions.streams;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

public class AarhusTrafficHttpStream extends RDFStream implements Runnable {
	private HttpGet httpGet;
	boolean stop = false;
	long sleep = 100000;
	private HttpClient httpClient;
	ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
		@Override
		public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
			int status = response.getStatusLine().getStatusCode();
			if (status >= 200 && status < 300) {
				HttpEntity entity = response.getEntity();
				return entity != null ? EntityUtils.toString(entity) : null;
			} else {
				throw new ClientProtocolException("Unexpected response status: " + status);
			}
		}

	};

	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	public AarhusTrafficHttpStream(ExecContext context, String uri) {
		super(context, uri);
		httpGet = new HttpGet(uri);
		httpClient = HttpClients.createDefault();
	}

	@Override
	public void run() {
		while (!stop) {
			System.out.println("Executing request " + httpGet.getRequestLine());
			try {
				String responseBody = httpClient.execute(httpGet, responseHandler);
				System.out.println("Response " + responseBody);

			} catch (ClientProtocolException e) {

				e.printStackTrace();
			} catch (IOException e) {

				e.printStackTrace();
			}
			if (sleep > 0) {
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public void stop() {
		stop = true;

	}

}
