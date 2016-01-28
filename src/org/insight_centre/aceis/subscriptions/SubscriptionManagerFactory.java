package org.insight_centre.aceis.subscriptions;

import org.deri.cqels.engine.ExecContext;

public class SubscriptionManagerFactory {
	private static SubscriptionManager sub;
	private static ExecContext context;

	public static ExecContext getContext() {
		return context;
	}

	public static void setContext(ExecContext context) {
		SubscriptionManagerFactory.context = context;
	}

	public static SubscriptionManager getSubscriptionManager() {
		if (sub == null) {
			try {
				sub = new SubscriptionManager(context, null);
				// sub.setAdptMode(null);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return sub;
	}
}
