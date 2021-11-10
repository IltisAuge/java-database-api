package de.iltisauge.databaseapi;

import java.util.logging.Logger;

public class DatabaseAPI {
	
	private static Logger LOGGER = Logger.getLogger("java-database-api");

	public static Logger getLogger() {
		return LOGGER;
	}
	
	public static void setLogger(Logger logger) {
		LOGGER = logger;
	}
}
