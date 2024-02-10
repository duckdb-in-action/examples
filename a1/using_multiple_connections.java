// tag::main[]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.duckdb.DuckDBConnection;

class using_multiple_connections {

	private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
	private static final String DUCKDB_URL
		= "jdbc:duckdb:readings.db"; // <.>

	public static void main(String... a) throws SQLException {

		var createTableStatement = """
			CREATE TABLE IF NOT EXISTS readings (
				id         INTEGER NOT NULL PRIMARY KEY,
			    created_on TIMESTAMP NOT NULL,
			    power      DECIMAL(10,3) NOT NULL
			)
			"""; // <.>

		try (
			var con = DriverManager
				.getConnection(DUCKDB_URL); // <.>
			var stmt = con.createStatement() // <.>
		) {
			stmt.execute(createTableStatement);
			var result = stmt // <.>
				.executeQuery("SELECT max(id) + 1 FROM readings");
			result.next();
			ID_GENERATOR.compareAndSet(0, result.getInt(1));
			result.close();

			var futures = Stream.generate(() ->
					CompletableFuture // <.>
						.runAsync(() -> insertNewReading(con)))
				.limit(20)
				.toArray(CompletableFuture[]::new);
			CompletableFuture.allOf(futures).join(); // <.>
		}
	}
	// end::main[]
	// tag::method[]
	static void insertNewReading(Connection connection) {
		var sql = "INSERT INTO readings VALUES (?, ?, ?)";
		var readOn = Timestamp.valueOf(LocalDateTime.now());
		var value = ThreadLocalRandom.current().nextDouble() * 100;

		try (
			var con = connection
				.unwrap(DuckDBConnection.class) // <.>
				.duplicate(); // <.>
			var stmt = con.prepareStatement(sql) // <.>
		) {
			stmt.setInt(1, ID_GENERATOR.getAndIncrement());
			stmt.setTimestamp(2, readOn);
			stmt.setDouble(3, value);
			stmt.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	// end::method[]
	// tag::main[]
}
// end::main[]
