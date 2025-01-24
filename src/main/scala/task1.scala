
import scala.io.Source
import java.sql.{Connection, DriverManager}

object Example {
  def main(args: Array[String]): Unit = {
    // Define file path, connection URL, and database credentials
    val filePath = "C:/Users/sayed/Desktop/Scala_Data/employee_data.csv"  
    val connectionUrl = "jdbc:mysql://localhost:3306/employeedatabse?serverTimezone=UTC"
    val username = "root"
    val password = "pass123"

    // Declare the connection variable outside the try block so it is accessible in the finally block
    var connection: Connection = null

    try {
      // Load MySQL JDBC driver (optional but recommended)
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Establish the database connection
      connection = DriverManager.getConnection(connectionUrl, username, password)

      // Read the CSV file (skip the header row)
      val lines = Source.fromFile(filePath).getLines().drop(1)  // Drop the header
      lines.foreach { line =>
        // Split the line by commas and trim each field
        val cols = line.split(",").map(_.trim)

        // Prepare the SQL query for inserting data
        val query = "INSERT INTO employees (id, name, department, salary) VALUES (?, ?, ?, ?)"
        val statement = connection.prepareStatement(query)

        // Set the parameters for the SQL query
        statement.setInt(1, cols(0).toInt)        // Set ID
        statement.setString(2, cols(1))           // Set Name
        statement.setString(3, cols(2))           // Set Department
        statement.setDouble(4, cols(3).toDouble)  // Set Salary

        // Execute the query
        statement.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace() // Handle any errors that occur
    } finally {
      // Ensure connection is closed after processing
      if (connection != null) {
        try {
          connection.close() // Close the connection if it's open
        } catch {
          case e: Exception => e.printStackTrace() // Handle exception while closing
        }
      }
    }
  }
}
