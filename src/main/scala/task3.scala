import breeze.plot._
import scala.io.Source

object Question3 {
  def main(args: Array[String]): Unit = {
    // CSV file path
    val filePath = "C:/Users/sayed/Desktop/MyScalaProject/Scala_Data/employee_data.csv" // Adjust path as needed

    try {
      // Read and parse the CSV file
      val lines = Source.fromFile(filePath).getLines()

      // Skip the header row
      val dataLines = lines.drop(1)

      // Parse data into (Department, Salary)
      val employees = dataLines.flatMap { line =>
        try {
          val cols = line.split(",").map(_.trim)
          if (cols.length >= 4) Some((cols(2), cols(3).toDouble)) // (Department, Salary)
          else None // Ignore invalid rows
        } catch {
          case _: NumberFormatException =>
            println(s"Skipping invalid row: $line")
            None
        }
      }.toList

      // Group by department and find the maximum salary in each group
      val topEarners = employees
        .groupBy(_._1) // Group by department
        .mapValues(_.map(_._2).max) // Find the maximum salary in each department

      // Extract departments and salaries for the chart
      val departments = topEarners.keys.toArray
      val salaries = topEarners.values.toArray

      // Create a Breeze figure for the bar chart
      val fig = Figure()
      val plt = fig.subplot(0)

      // X values as indices for departments
      val xValues = departments.indices.map(_.toDouble).toArray

      // Draw vertical bars for each department
      for (i <- xValues.indices) {
        plt += plot(Array(xValues(i), xValues(i)), Array(0.0, salaries(i)), '-') // Draw bar
      }

      // Adjust axis properties
      plt.xlabel = "Department"
      plt.ylabel = "Top Salary"
      plt.title = "Top Earners' Salaries by Department"
      plt.xlim = (xValues.min - 0.5, xValues.max + 0.5)
      plt.ylim = (0, salaries.max * 1.1)

      // Display department names manually
      println("Department labels:")
      departments.zipWithIndex.foreach { case (dep, idx) =>
        println(s"$dep at position $idx")
      }

      // Refresh the chart to display
      fig.refresh()
      
    } catch {
      case e: Throwable =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
