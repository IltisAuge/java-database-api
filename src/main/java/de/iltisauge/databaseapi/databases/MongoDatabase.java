package de.iltisauge.databaseapi.databases;

import java.util.Arrays;
import java.util.logging.Level;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import de.iltisauge.databaseapi.Credential;
import de.iltisauge.databaseapi.Database;
import de.iltisauge.databaseapi.DatabaseAPI;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MongoDatabase implements Database {

	@Getter
	private final Credential credential;
	@Getter
	private com.mongodb.client.MongoDatabase mongoDatabase;
	private MongoClient mongoClient;
	
	@Override
	public boolean connect() {
		try {
			final MongoCredential mongoCredential = MongoCredential.createCredential(credential.getUsername(), credential.getDatabase(), credential.getPassword().toCharArray());
			mongoClient = new MongoClient(new ServerAddress(credential.getHostname(), credential.getPort()), Arrays.asList(mongoCredential));
			mongoDatabase = mongoClient.getDatabase(credential.getDatabase());
		} catch (Exception exception) {
			DatabaseAPI.getLogger().log(Level.WARNING, "Error while connecting to database `" + credential.getDatabase() + "` via " + getClass().getName() + ": ", exception);
			return false;
		}
		return true;
	}

	@Override
	public void disconnect() {
		try {
			if (isConnected()) {
				mongoClient.close();
				mongoClient = null;
			}
		} catch (Exception exception) {
			DatabaseAPI.getLogger().log(Level.WARNING, "Error while disconnecting from database `" + credential.getDatabase() + "` via " + getClass().getName() + ": ", exception);
		}
	}

	@Override
	public boolean isConnected() {
		try {
			return mongoClient != null;
		} catch (Exception exception) {
			DatabaseAPI.getLogger().log(Level.WARNING, "Error while checking isConnected for database `" + credential.getDatabase() + "`: ", exception);
			return false;
		}
	}
	
	public void createCollection(String name) {
		mongoDatabase.createCollection(name);
	}
}
