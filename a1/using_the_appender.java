import java.util.ArrayList;
import java.util.List;

// tag::writing[]
	import java.sql.DriverManager;
	import java.sql.SQLException;
	import java.util.concurrent.ThreadLocalRandom;
	import org.duckdb.DuckDBConnection;

// end::writing[]

class using_the_appender {

	// tag::reading[]
	private record WeatherStation(String id, double avgTemperature) {
		// end::reading[]
		double measurement() {
			double m = ThreadLocalRandom.current().nextGaussian(avgTemperature, 10);
			return Math.round(m * 10.0) / 10.0;
		}
		// tag::reading[]
	}

	static List<WeatherStation> weatherStations() throws SQLException {

		var query = """
				SELECT City AS id, 
				       cast(replace(
				         trim(
				           regexp_extract(Year,'(.*)\\n.*', 1)
				          ), '−', '-') AS double) 
				         AS avgTemperature
				FROM 'weather/*.parquet'
			""";  // <.>
		var weatherStations = new ArrayList<WeatherStation>();
		try ( // <.>
		      var con = DriverManager
			      .getConnection("jdbc:duckdb:");
		      var stmt = con.createStatement();
		      var resultSet = stmt.executeQuery(query)
		) {
			while (resultSet.next()) { // <.>
				var id = resultSet.getString("id");
				var avgTemperature = resultSet.getDouble("avgTemperature");
				weatherStations.add(new WeatherStation(id, avgTemperature));
			}
		}

		return weatherStations;
	}
	// end::reading[]


	// tag::writing[]
	static void generateData(int size) throws SQLException {

		var stations = weatherStations();
		var numStations = stations.size();

		try (var con = DriverManager.getConnection("jdbc:duckdb:weather.db")
			.unwrap(DuckDBConnection.class) // <.>
		) {
			// end::writing[]
			try (var stmt = con.createStatement()) {
				stmt.execute("""
					CREATE TABLE IF NOT EXISTS weather (
						id VARCHAR(64), measurement DECIMAL(3,1)
					)
					""");
				stmt.executeUpdate("DELETE FROM weather");
			}
			// tag::writing[]

			var rand = ThreadLocalRandom.current();
			long start = System.currentTimeMillis();
			try (var appender = con.createAppender(
					DuckDBConnection.DEFAULT_SCHEMA, // <.>
					"weather") // <.>
			) {
				for (int i = 0; i < size; ++i) {
					if (i > 0 && i % 50_000_000 == 0) {
						appender.flush(); // <.>
						// end::writing[]
						var elapsed = System.currentTimeMillis() - start;
						System.out.printf("Wrote %,d measurements in %s ms%n", i, elapsed);
						// tag::writing[]
					}
					var station = stations.get(rand.nextInt(numStations));
					appender.beginRow(); // <.>
					appender
						.append(station.id()); // <.>
					appender
						.append(station.measurement());
					appender.endRow(); // <.>
				}
			}
		}
	}
	// end::writing[]

	public static void main(String... args) throws SQLException {
		int size = 0;
		try {
			size = Integer.parseInt(args[0]);
		} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
			System.out.println("Usage: using_the_appender <number of records to create>");
			System.exit(1);
		}

		generateData(size);
	}
}
