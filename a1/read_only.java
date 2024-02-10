import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import org.duckdb.DuckDBDriver;

class read_only {

	public static void main(String... args) throws SQLException {

		var properties = new Properties(); // <.>
		properties.setProperty( // <.>
			DuckDBDriver.DUCKDB_READONLY_PROPERTY, "true");
		properties.setProperty(
			DuckDBDriver.JDBC_STREAM_RESULTS, "true");

		var query = """
			SELECT id AS station_name,
				   MIN(measurement) AS min,
				   CAST(AVG(measurement) AS DECIMAL(8,1)) AS mean,
			       MAX(measurement) AS max
			FROM weather
			GROUP BY station_name
			ORDER BY station_name
			""";
		var url = "jdbc:duckdb:weather.db";

		try (
			var con = DriverManager
				.getConnection(url, properties); // <.>
			var stmt = con.createStatement();
			var result = stmt.executeQuery(query)
		) {
			boolean first = true;
			System.out.print("{");
			while (result.next()) {
				if(!first) {
					System.out.print(", ");
				}
				var station = result.getString("station_name");
				var min = result.getDouble("min");
				var mean = result.getDouble("mean");
				var max = result.getDouble("max");
				System.out.printf(
					Locale.ENGLISH, "%s=%3.2f/%3.2f/%3.2f",
					station, min, mean, max);
				first = false;
			}
		}
		System.out.println("}");
	}
}
