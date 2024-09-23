import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.Seq

object Main {

  def main(args: Array[String]): Unit = {

    //Spark setup and load the DF
    manageSparkOutputs()

    val spark = SparkSession.builder.appName("Flight Data Assignment").master("local").getOrCreate()

    showComponentVersions(spark.version)

    val (flightData, passengerData) = CSVUtils.loadDataFrames(spark)

    //Question 1: Find the total number of flights for each month.
    groupFlightsByDate(dataFrame = flightData, groupByFunction = month(col("date")), groupName = "Month", sparkSession = spark, csvPath = "../question1.csv")

    //Question 2: Find the names of the 100 most frequent flyers.
    mostFrequentFlyers(dfPassengers = passengerData, dfFlights = flightData, sparkSession = spark, csvPath = "../question2.csv")

    //Question 3: Find the greatest number of countries a passenger has been in without being in the UK.
    longestCountriesWithoutUK(dfFlights = flightData, sparkSession = spark, csvPath = "../question3.csv")

    //Question 4: Find the passengers who have been on more than 3 flights together.
    numberFlightsTogether(dfFlights = flightData, minNumFlightsTgt = 3, sparkSession = spark, csvPath = "../question4.csv")


    //Extra: Find the passengers who have been on more than N flights together within the range (from,to).
    val fromDate = "2017-01-01"
    val toDate = "2017-02-01"
    val csvPath = "../question4_"+fromDate+"_"+toDate+".csv"
    flownTogether(atLeastNTimes = 5, from = fromDate, to = toDate, dfFlights = flightData, sparkSession = spark, csvPath = csvPath)

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
    println(s"Spark version: " + version)
    println(s"JDK version: ${System.getProperty("java.version")}\n\n")
  }


  /**
   * Configure Spark and Hadoop log levels to prevent non-critical warnings from being displayed.
   * This method sets the log level to ERROR for several Spark and Hadoop components,
   * in order to minimise noise in the console and prevent repetitive warnings from interfering with execution or display.
   * interfere with the execution or display of results.
   */
  def manageSparkOutputs(): Unit = {
    Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop.security.UserGroupInformation").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.util.Utils").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  }

  /**
   * Generic method to group flights by a date criterion (month, year, day, etc.)
   *
   * @param dataFrame       Dataset[FlightData] Dataset of flight data
   * @param groupByFunction Grouping function for the date column (month, year, day, etc.)
   * @param groupName       Name of the group (e.g., ‘Month’, ‘Year’, etc.)
   * @param sparkSession    SparkSession to use for reading and writing data.
   * @param csvPath         Path to the CSV file where the result will be saved or loaded from.
   * @return DataFrame with the total number of flights grouped by the selected criterion
   */
  def groupFlightsByDate(dataFrame: Dataset[FlightData], groupByFunction: => Column, groupName: String, sparkSession: SparkSession, csvPath: String): Unit = {
    if (!CSVUtils.checkAndLoadCSV(csvPath = csvPath, sparkSession = sparkSession)) {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val groupedData = dataFrame.groupBy(groupByFunction.as(groupName))
        .agg(count("flightId").as("Number of Flights"))
        .orderBy(groupName)

      CSVUtils.saveDataFrameAsCSV(groupedData, csvPath)
    }
  }



  /**
   * Finds and exports the top 100 most frequent flyers based on the number of flights taken.
   *
   * This method groups the flight data by `passengerId`, counts the number of flights per passenger,
   * and orders the result by the passengers with the highest number of flights. The result is then
   * joined with the passenger dataset to enrich the data with passenger details, and finally, the top
   * 100 frequent flyers are saved to a CSV file.
   *
   * @param dfPassengers Dataset[Passenger] containing passenger details (e.g., name, ID).
   * @param dfFlights    Dataset[FlightData] containing flight data including passenger IDs.
   * @param sparkSession  SparkSession to use for reading and writing data.
   * @param csvPath       Path to the CSV file where the result will be saved or loaded from.
   */
  def mostFrequentFlyers(dfPassengers: Dataset[Passenger], dfFlights: Dataset[FlightData], sparkSession: SparkSession, csvPath: String): Unit = {
    if (!CSVUtils.checkAndLoadCSV(csvPath = csvPath, sparkSession = sparkSession)) {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val freq_flyers = dfFlights.groupBy("passengerId")
        .agg(count("flightId").as("Number of Flights"))
        .orderBy(desc("Number of Flights"))
        .limit(100)
        .join(dfPassengers, "passengerId")
        .orderBy(desc("Number of Flights"))

      CSVUtils.saveDataFrameAsCSV(freq_flyers, csvPath)
    }

  }

  /**
   * This method calculates the longest consecutive flight sequence for each passenger without visiting the UK
   * and saves the result in a CSV file. If the CSV file already exists, it loads and displays the data from it.
   *
   * @param dfFlights   Dataset[FlightData] containing flight details for each passenger.
   * @param sparkSession SparkSession to use for reading and writing data.
   * @param csvPath      Path to the CSV file where the result will be saved or loaded from.
   */
  def longestCountriesWithoutUK(dfFlights: Dataset[FlightData], sparkSession: SparkSession, csvPath: String): Unit = {
    if (!CSVUtils.checkAndLoadCSV(csvPath = csvPath, sparkSession = sparkSession)) {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      //Make a column about if passenger was in Uk or not
      val dfFlightsInvolvesUk = dfFlights.withColumn("involvesUk",
          when(col("from") === "uk" || col("to") === "uk", 1).otherwise(0))
        .orderBy("date")

      //Group by passenger and collect the flight information
      val passengerFlights = dfFlightsInvolvesUk.groupBy("passengerId")
        .agg(collect_list(struct("from", "to", "date", "involvesUk")).as("flights"))

      val passengers_longestRun_RDD = passengerFlights.rdd.map(row => {
        val passengerId = row.getAs[Int]("passengerId")
        val flights = row.getAs[Seq[Row]]("flights")

        var maxCount: Int = 0
        var visitedCountries: Set[String] = Set()

        flights.foreach { flight =>
          val involvesUk = flight.getAs[Int]("involvesUk")
          val fromCountry = flight.getAs[String]("from")
          val toCountry = flight.getAs[String]("to")

          if (involvesUk == 0) {
            visitedCountries += fromCountry
            visitedCountries += toCountry
            if (visitedCountries.size > maxCount) maxCount = visitedCountries.size
          } else
            visitedCountries = Set()
        }

        (passengerId, maxCount)

      }
      )
      import sparkSession.implicits._
      val passengers_longestRunDF = passengers_longestRun_RDD.toDF("Passenger ID", "Longest Run")

      CSVUtils.saveDataFrameAsCSV(passengers_longestRunDF, csvPath)

    }
  }

  /**
   * This method calculates the number of flights where two passengers have been together and saves the result in a CSV file.
   * If the CSV file already exists, it will be loaded and displayed. If it doesn't exist, the data will be processed and saved.
   *
   * @param dfFlights           Dataset[FlightData] containing flight data for each passenger.
   * @param minNumFlightsTgt    Minimum number of flights two passengers must have been together to be included in the result.
   * @param sparkSession        SparkSession for Spark operations.
   * @param csvPath             Path to the CSV file where the result will be saved or loaded from.
   */
  def numberFlightsTogether(dfFlights: Dataset[FlightData], minNumFlightsTgt: Int, sparkSession: SparkSession, csvPath: String): Unit = {
    if (!CSVUtils.checkAndLoadCSV(csvPath = csvPath, sparkSession = sparkSession)) {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val groupedFlightsId = dfFlights.groupBy("flightId")
        .agg(collect_set("passengerId").as("passengers"))

      //Make rows for each passenger-passenger that they have been in the same flight AND clean with same passengerIds
      val passengerPairs = groupedFlightsId
        .withColumn("passenger1 ID", explode(col("passengers")))
        .withColumn("passenger2 ID", explode(col("passengers")))
        .filter(col("passenger1 ID") =!= col("passenger2 ID"))

      //Group by both passengerIds and count them (Delete less than min_flights_together).
      val minFlightsTgtPassengers = passengerPairs.groupBy("Passenger1 ID", "Passenger2 ID")
        .agg(countDistinct("flightId").as("Number of flights together"))
        .filter(col("Number of flights together") > minNumFlightsTgt)


      //Ordering columns Passenger1 ID & Passenger2 ID and reduce by key to take the unique rows.
      val mapResult = minFlightsTgtPassengers.rdd.map(row => {
        val passenger1 = row.getAs[Int]("Passenger1 ID")
        val passenger2 = row.getAs[Int]("Passenger2 ID")

        val key = if(passenger1 < passenger2)  (passenger1, passenger2) else (passenger2, passenger1)
        (key,row)
      })
      val reducedRDD = mapResult.reduceByKey((row1, row2) => row1).map(_._2)
      val reducedDataFrame = sparkSession.createDataFrame(reducedRDD,minFlightsTgtPassengers.schema)

      CSVUtils.saveDataFrameAsCSV(reducedDataFrame, csvPath)
    }

  }


  /**
   * This method calculates which passengers have flown together at least a specified number of times
   * within a given date range. It filters flights based on the date range, groups passengers by flight,
   * and then identifies pairs of passengers who have been on the same flight.
   * For each pair, it counts the number of times they flew together and determines the first and last date
   * they were on the same flight. If the CSV file already exists, it loads the data instead of recalculating it.
   *
   * @param atLeastNTimes The minimum number of times two passengers must have flown together to be included in the result.
   * @param from          Start date of the date range for filtering flights.
   * @param to            End date of the date range for filtering flights.
   * @param dfFlights    Dataset[FlightData] containing flight data, including flight IDs and passenger IDs.
   * @param sparkSession  SparkSession to be used for performing operations in Spark.
   * @param csvPath       Path to the CSV file where the result will be saved or loaded from.
   */
  def flownTogether(atLeastNTimes: Int, from: String, to: String, dfFlights: Dataset[FlightData], sparkSession: SparkSession, csvPath : String): Unit = {

    if (!CSVUtils.checkAndLoadCSV(csvPath = csvPath, sparkSession = sparkSession)) {
      println(s"CSV file '$csvPath' does not exist. Generating and saving data...")

      val groupedFlightsId = dfFlights.filter(col("date") >= from && col("date") <= to)
        .groupBy("flightId")
        .agg(collect_set("passengerId").as("passengers"), first(col("date")).as("flightDate"))

      val passengerPairs = groupedFlightsId
        .withColumn("Passenger1 ID", explode(col("passengers")))
        .withColumn("Passenger2 ID", explode(col("passengers")))
        .filter(col("Passenger1 ID") =!= col("Passenger2 ID"))
        .select("Passenger1 ID", "Passenger2 ID", "flightDate")

      val minFlightsTgtPassengers = passengerPairs.groupBy("Passenger1 ID", "Passenger2 ID")
        .agg(count("*").as("Number of flights together"), min("flightDate").as("From"), max("flightDate").as("To"))
        .filter(col("Number of flights together") > atLeastNTimes)

      //Ordering columns Passenger1 ID & Passenger2 ID and reduce by key to take the unique rows.
      val mapResult = minFlightsTgtPassengers.rdd.map(row => {
        val passenger1 = row.getAs[Int]("Passenger1 ID")
        val passenger2 = row.getAs[Int]("Passenger2 ID")

        val key = if(passenger1 < passenger2)  (passenger1, passenger2) else (passenger2, passenger1)
        (key,row)
      })
      val reducedRDD = mapResult.reduceByKey((row1, row2) => row1).map(_._2)
      val reducedDataFrame = sparkSession.createDataFrame(reducedRDD,minFlightsTgtPassengers.schema)

      CSVUtils.saveDataFrameAsCSV(reducedDataFrame, csvPath)

    }
  }


}


