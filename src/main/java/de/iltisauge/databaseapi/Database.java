package de.iltisauge.databaseapi;

public interface Database {
	
	/**
	 * 
	 * @return the credentials to access the database.
	 */
	Credential getCredential();

	void setCredential(Credential credential);

	/**
	 * Only connects to the database, if not already connected.
	 * @return true, if a connection to the database could be created, otherwise false.
	 */
	default boolean tryToConnect() {
		if (!isConnected()) {
			return connect();
		}
		return false;
	}
	
	/**
	 * Tries to connect to the database.
	 * @return true, if a connection to the database could be created, otherwise false.
	 */
	boolean connect();
	
	/**
	 * 
	 * @return true, if the connection to the database is stable, otherwise false.
	 */
	boolean isConnected();
	
	/**
	 * Closes the connection to the database.
	 */
	void disconnect();

}
