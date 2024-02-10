import java.sql.DriverManager;
import java.sql.SQLException;

class simple {

	public static void main(String... a) throws SQLException {

		var query = "SELECT * FROM duckdb_settings() ORDER BY name";
		try (
				var con = DriverManager
						.getConnection("jdbc:duckdb:"); // <.>
				var stmt = con.createStatement(); // <.>
				var resultSet = stmt.executeQuery(query) // <.>
		) {
			while (resultSet.next()) { // <.>
				System.out.printf("%s %s%n",
						resultSet.getString("name"),
						resultSet.getString("value"));
			}
		}
	}
}
