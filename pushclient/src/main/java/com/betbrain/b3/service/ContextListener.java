package com.betbrain.b3.service;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.betbrain.b3.data.DynamoWorker;

public class ContextListener implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent event) {
		
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		DynamoWorker.initBundleCurrent();
	}

}
