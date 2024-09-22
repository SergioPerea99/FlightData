import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.Seq

object Main {

  def main(args: Array[String]): Unit = {

    //Spark setup and load the DF
    manageSparkOutputs()

    val spark = SparkSession.builder.appName("Flight Data Assignment").master("local").getOrCreate()

    showComponentVersions(spark.version)

    val (flightData, passengerData) = loadDataFrames(spark)

    //Question 1: Find the total number of flights for each month.
    groupFlightsByDate(dataFrame = flightData, groupByFunction = month(col("date")), groupName = "Month", sparkSession = spark)

    //Question 2: Find the names of the 100 most frequent flyers.
    mostFrequentFlyers(df_passengers = passengerData, df_flights = flightData, sparkSession = spark)

    //Question 3: Find the greatest number of countries a passenger has been in without being in the UK
    longestCountriesWithoutUK(df_flights = flightData, sparkSession = spark)

    spark.stop()
  }

  /**
   * Displays the component versions used in the Flight Data Coding Assignment.
   * It prints the versions of Scala, Spark, and the JDK currently being used in the environment.
   * This method helps ensure compatibility with the requirements of the assignment.
   *
   * @param version The version of Spark to be displayed.
   */
  private def showComponentVersions(version: String): Unit = {
    println("\nFlight Data Coding Assignment Requirements")
    println(s"Scala version: ${util.Properties.versionNumberString}")
    println(s"Spark version: "+ version)
    println(s"JDK version: ${System.getProperty("java.version")}\n\n")
  }


  /**
 * Configure Spark and Hadoop log levels to prevent non-critical warnings from being displayed.
 * This method sets the log level to ERROR for several Spark and Hadoop components,
 * in order to minimise noise in the console and prevent repetitive warnings from interfering with execution or display.
 * interfere with the execution or display of results.
 */
 private def manageSparkOutputs(): Unit = {
    Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop.security.UserGroupInformation").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.util.Utils").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  }

  /**
   * Private method that loads the CSV files, checks the DataFrames schema and matches the columns to the necessary types, returning typed Datasets for FlightData and Passenger.
   * to the necessary types, returning typed Datasets for FlightData and Passenger.
   *
   * @param spark Spark session
   * @return A tuple with the typed Datasets (Dataset[FlightData], Dataset[Passenger])
   */
  private def loadDataFrames(spark: SparkSession): (Dataset[FlightData], Dataset[Passenger]) = {
    import spark.implicits._
    // Loading the data from CSV
    println("Loading flightData.csv and passengers.csv...\n")
    val flightDataDF = spark.read.option("header", "true").csv("flightData.csv")
    val passengerDataDF = spark.read.option("header", "true").csv("passengers.csv")

    // Showing the schema for each DF
    println("Showing schema from DF of flightData.csv...")
    flightDataDF.printSchema()
    println("Showing schema from DF of passenger.csv...")
    passengerDataDF.printSchema()

    // Casting rows & changing to typed datasets
    println("Making typed datasets: flightData & passengerData...")
    val flightData = flightDataDF
      .withColumn("passengerId", col("passengerId").cast("int"))
      .withColumn("flightId", col("flightId").cast("int"))
      .as[FlightData]

    val passengerData = passengerDataDF
      .withColumn("passengerId", col("passengerId").cast("int"))
      .as[Passenger]

    //Printing samples with a seed 1234
    println("Taking a sample from flightData with seed 1234: "+flightData.rdd.takeSample(withReplacement = false, 1, 1234).mkString("(", ", ", ")"))
    println("Taking a sample from passengerData with seed 1234: "+passengerData.rdd.takeSample(withReplacement = false, 1, 1234).mkString("(", ", ", ")"))

    // Return the datasets
    (flightData, passengerData)
  }

  /**
   * Generic method to group flights by a date criterion (month, year, day, etc.)
   * @param dataFrame Dataset[FlightData] Dataset of flight data
   * @param groupByFunction Grouping function for the date column (month, year, day, etc.)
   * @param groupName Name of the group (e.g., ‘Month’, ‘Year’, etc.)
   * @param sparkSession SparkSession to use for reading and writing data.
   * @return DataFrame with the total number of flights grouped by the selected criterion
   */
  private def groupFlightsByDate(dataFrame: Dataset[FlightData], groupByFunction: => Column, groupName: String, sparkSession: SparkSession): Unit = {
    val csvPath = "../question1.csv"

    val file = new File(csvPath)

    if (file.exists()) {
      println(s"CSV file '$csvPath' already exists. Loading data from CSV...")
      val loadedData = sparkSession.read.option("header", "true").option("delimiter",";").csv(csvPath)
      loadedData.show()
    } else {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val groupedData = dataFrame.groupBy(groupByFunction.as(groupName))
        .agg(count("flightId").as("Number of Flights"))
        .orderBy(groupName)

      saveDataFrameAsCSV(groupedData, csvPath)
    }
  }


  /**
   * Method to save any DataFrame to a CSV file.
   *
   * @param dataFrame The DataFrame to save.
   * @param outputPath The path where the CSV file will be saved.
   */
  private def saveDataFrameAsCSV(dataFrame: DataFrame, outputPath: String): Unit = {

    val header = dataFrame.columns.mkString(";")
    val rows = dataFrame.collect().map(row => row.toSeq.map(_.toString).toArray)
    val file = new BufferedWriter(new FileWriter(outputPath))
    file.write(header)
    file.newLine()
    for (row <- rows) {
      file.write(row.mkString(";"))
      file.newLine()
    }
    file.close()
  }


  /**
   * Finds and exports the top 100 most frequent flyers based on the number of flights taken.
   *
   * This method groups the flight data by `passengerId`, counts the number of flights per passenger,
   * and orders the result by the passengers with the highest number of flights. The result is then
   * joined with the passenger dataset to enrich the data with passenger details, and finally, the top
   * 100 frequent flyers are saved to a CSV file.
   *
   * @param df_passengers Dataset[Passenger] containing passenger details (e.g., name, ID).
   * @param df_flights Dataset[FlightData] containing flight data including passenger IDs.
   * @param sparkSession SparkSession to use for reading and writing data.
   */
  private def mostFrequentFlyers(df_passengers: Dataset[Passenger], df_flights: Dataset[FlightData], sparkSession: SparkSession): Unit = {
    val csvPath = "../question2.csv"

    val file = new File(csvPath)

    if (file.exists()) {
      println(s"CSV file '$csvPath' already exists. Loading data from CSV...")
      val loadedData = sparkSession.read.option("header", "true").option("delimiter",";").csv(csvPath)
      loadedData.show()
    } else {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val freq_flyers = df_flights.groupBy("passengerId")
        .agg(count("flightId").as("Number of Flights"))
        .orderBy(desc("Number of Flights"))
        .limit(100)
        .join(df_passengers,"passengerId")
        .orderBy(desc("Number of Flights"))

      saveDataFrameAsCSV(freq_flyers,csvPath)
    }

  }

  /**
   * This method calculates the longest consecutive flight sequence for each passenger without visiting the UK
   * and saves the result in a CSV file. If the CSV file already exists, it loads and displays the data from it.
   *
   * @param df_flights Dataset[FlightData] containing flight details for each passenger.
   * @param sparkSession SparkSession to use for reading and writing data.
   */
  private def longestCountriesWithoutUK(df_flights: Dataset[FlightData], sparkSession: SparkSession):Unit  = {
    val csvPath = "../question3.csv"

    val file = new File(csvPath)

    if (file.exists()) {
      println(s"CSV file '$csvPath' already exists. Loading data from CSV...")
      val loadedData = sparkSession.read.option("header", "true").option("delimiter",";").csv(csvPath)
      loadedData.show()
    } else {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      //Make a column about if passenger was in Uk or not
      val df_flights_involvesUk = df_flights.withColumn("involvesUk",
        when(col("from") === "uk" || col("to") === "uk",1).otherwise(0))
        .orderBy("date")

      //Group by passenger and collect the flight information
      val passenger_flights = df_flights_involvesUk.groupBy("passengerId")
        .agg(collect_list(struct("from","to","date","involvesUk")).as("flights"))

      val passengers_longestRun_RDD = passenger_flights.rdd.map( row => {
        val passengerId = row.getAs[Int]("passengerId")
        val flights = row.getAs[Seq[Row]]("flights")

        var currentCount = 0
        var maxCount: Int = 0

        flights.foreach{ flight =>
          val involvesUk = flight.getAs[Int]("involvesUk")
          if (involvesUk == 0) {
            currentCount += 1
            if(currentCount > maxCount) maxCount = currentCount
          }else
            currentCount = 0
        }

        (passengerId, maxCount)

      }
      )

      val passengers_longestRunDF = sparkSession.createDataFrame(passengers_longestRun_RDD)

      saveDataFrameAsCSV(passengers_longestRunDF,csvPath)

    }
  }

}


