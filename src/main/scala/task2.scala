import java.sql.{Connection, DriverManager, ResultSet}

object TopEarner {
  def main(args: Array[String]): Unit = {
    val connectionUrl = "jdbc:mysql://localhost:3306/employeedatabse?serverTimezone=UTC"
    val username = "root"
    val password = "pass123"

    var connection: Connection = null
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection(connectionUrl, username, password)

      val query = "SELECT department, name, salary FROM employees WHERE (department, salary) IN (SELECT department, MAX(salary) FROM employees GROUP BY department)"
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)

      println("Top Earners by Department:")
      while (resultSet.next()) {
        val department = resultSet.getString("department")
        val name = resultSet.getString("name")
        val salary = resultSet.getDouble("salary")
        println(s"Department: $department, Name: $name, Salary: $salary")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }
}
