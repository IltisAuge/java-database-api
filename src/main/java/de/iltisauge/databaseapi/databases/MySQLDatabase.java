package de.iltisauge.databaseapi.databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;

import de.iltisauge.databaseapi.Credential;
import de.iltisauge.databaseapi.Database;
import de.iltisauge.databaseapi.DatabaseAPI;
import de.iltisauge.databaseapi.PrimaryKey;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class MySQLDatabase implements Database {

	@Getter
	@Setter
	private Credential credential;
	private Connection connection;

	public MySQLDatabase(Credential credential) {
		this.credential = credential;
	}

	@Override
	public boolean connect() {
		try {
			connection = DriverManager.getConnection("jdbc:mysql://" + credential.getHostname() + ":" + credential.getPort() + "/" + credential.getDatabase(),
					credential.getUsername(), credential.getPassword());
		} catch (SQLException exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "Could not connect to database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
			return false;
		}
		return isConnected();
	}

	@Override
	public boolean isConnected() {
		try {
			return connection != null && !connection.isClosed() && connection.isValid(2);
		} catch (SQLException exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while checking connection to database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
			return false;
		}
	}

	@Override
	public void disconnect() {
		try {
			connection.close();
			connection = null;
		} catch (SQLException exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while checking closing connection to database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
		}
	}
	
	/**
	 * 
	 * Creates a new {@link PreparedStatement} with the given query and values.<br>
	 * Tries to execute the PreparedStatement.
	 * 
	 * @param query
	 * @param values
	 * @return true, if the execution of the PreparedStatement was successful, otherwise false.
	 */
	public boolean execute(String query, Object... values) {
		try {
			final PreparedStatement preparedStatement = connection.prepareStatement(query);
			for (int i = 1; i <= values.length; i++) {
				preparedStatement.setObject(i, values[i - 1]);
			}
			preparedStatement.executeUpdate();
			return true;
		} catch (Exception exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while executing query '" + query + "' to database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
			return false;
			
		}
	}
	
	/**
	 * Creates a new {@link PreparedStatement} with the given query and values.<br>
	 * Tries to execute the PreparedStatement.
	 * 
	 * @param query
	 * @param values
	 * @return a {@link ResultSet}, if no errors occored, otherwise null.
	 */
	public ResultSet getResult(String query, Object... values) {
		try {
			final PreparedStatement preparedStatement = prepareStatement(connection.prepareStatement(query), values);
			return preparedStatement.executeQuery();
		} catch (Exception exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while getting result '" + query + "' from database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
			return null;
			
		}
	}
	
	/**
	 * 
	 * Creates a new {@link PreparedStatement} with the given query and values.<br>
	 * Tries to execute the PreparedStatement asynchronously <b>asynchronously</b>.
	 * 
	 * 
	 * @param query
	 * @param values
	 * @return true, if the execution of the PreparedStatement was successful, otherwise false.
	 */
	public boolean executeAsync(String query, Object... values) {
		final FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
			
			@Override
			public Boolean call() throws Exception {
				return execute(query, values);
			}
		});
		return executeAndGetFromFuture(futureTask);
	}
	
	/**
	 * Creates a new {@link PreparedStatement} with the given query and values.<br>
	 * Tries to execute the PreparedStatement <b>asynchronously</b>.
	 * 
	 * @param query
	 * @param values
	 * @return a {@link ResultSet}, if no errors occored, otherwise null.
	 */
	public ResultSet getResultAsync(String query, Object... values) {
		final FutureTask<ResultSet> futureTask = new FutureTask<ResultSet>(new Callable<ResultSet>() {
			
			@Override
			public ResultSet call() throws Exception {
				return getResult(query, values);
			}
		});
		return executeAndGetFromFuture(futureTask);
	}
	
	private <T> T executeAndGetFromFuture(FutureTask<T> futureTask) {
		final ExecutorService executorService = Executors.newCachedThreadPool();
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (InterruptedException | ExecutionException exception) {
			DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while executing and getting from FutureTask for database '" + credential.getDatabase() + "' on " +
					credential.getHostname() + ":" + credential.getPort(), exception);
		}
		return null;
	}
	
	private PreparedStatement prepareStatement(PreparedStatement preparedStatement, Object... values) {
		for (int i = 1; i <= values.length; i++) {
			try {
				preparedStatement.setObject(i, values[i - 1]);
			} catch (SQLException exception) {
				DatabaseAPI.getLogger().log(Level.SEVERE, "An error occored while preparing statement on database '" + credential.getDatabase() + "' on " +
						credential.getHostname() + ":" + credential.getPort(), exception);
			}
		}
		return preparedStatement;
	}
	
	/**
	 * 
	 * Creates a table if no table with the given name already exists.<br>
	 * <b>Give a column name like this: columnName char(36)</b>
	 */
	public void createTable(String name, String... columns) {
		createTable(name, new PrimaryKey(""), columns);
	}
	
	/**
	 * 
	 * Creates a table if no table with the given name already exists.<br>
	 * <b>Give a column name like this: columnName char(36)</b>
	 */
	public void createTable(String name, PrimaryKey primaryKey, String... columns) {
		createTable(name, new String[] { primaryKey.getValue() }, columns);
	}
	
	/**
	 * 
	 * Creates a table if no table with the given name already exists.<br>
	 * <b>Give a column name like this: columnName char(36)</b>
	 */
	public void createTable(String name, String[] primaryKeys, String... columns) {
		String s = "";
		for (String c : columns) {
			s += ", " + c;
		}
		s = s.substring(2);
		String primaryKeyString = "";
		if (primaryKeys != null) {
			for (int i = 0; i < primaryKeys.length; i++) {
				primaryKeyString += ((i == 0 ? "" : ", ") + ("`" + primaryKeys[i] + "`"));
			}
		}
		execute("CREATE TABLE IF NOT EXISTS `" + name + "` (" + s + (primaryKeys == null ? "" : ", PRIMARY KEY(" + primaryKeyString + ")") + ");");
	}
}