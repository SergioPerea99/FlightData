import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}

object CSVUtils {


  /**
   * Private method that loads the CSV files, checks the DataFrames schema and matches the columns to the necessary types, returning typed Datasets for FlightData and Passenger.
   * to the necessary types, returning typed Datasets for FlightData and Passenger.
   *
   * @param spark Spark session
   * @return A tuple with the typed Datasets (Dataset[FlightData], Dataset[Passenger])
   */
  def loadDataFrames(spark: SparkSession): (Dataset[FlightData], Dataset[Passenger]) = {
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

    //Validate data
    validateFlightData(flightDataDF)
    validatePassengerData(passengerDataDF)

    // Casting rows & changing to typed datasets
    println("Typed datasets: flightData & passengerData")
    val flightData = flightDataDF
      .withColumn("passengerId", col("passengerId").cast("int"))
      .withColumn("flightId", col("flightId").cast("int"))
      .as[FlightData]

    val passengerData = passengerDataDF
      .withColumn("passengerId", col("passengerId").cast("int"))
      .as[Passenger]

    // Return the datasets
    (flightData, passengerData)
  }

  /**
   * This method checks if a CSV file exists at the specified path.
   * If the file exists, it loads the data from the CSV and displays it.
   * If the file does not exist, it returns false, indicating that the data needs to be generated.
   *
   * @param csvPath      Path to the CSV file that should be checked and loaded if it exists.
   * @param sparkSession SparkSession used to perform the CSV reading operation.
   * @return Boolean value indicating whether the CSV file was found and loaded (true) or not (false).
   */
  def checkAndLoadCSV(csvPath: String, sparkSession: SparkSession): Boolean = {
    val file = new File(csvPath)
    if (file.exists()) {
      println(s"CSV file '$csvPath' already exists. Loading data from CSV...")
      val loadedData = sparkSession.read.option("header", "true").option("delimiter", ";").csv(csvPath)
      loadedData.show()
      true
    } else {
      false
    }
  }

  /**
   * Method to save any DataFrame to a CSV file.
   *
   * @param dataFrame  The DataFrame to save.
   * @param outputPath The path where the CSV file will be saved.
   */
  def saveDataFrameAsCSV(dataFrame: DataFrame, outputPath: String): Unit = {

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
   * Validates the flight data DataFrame to ensure it contains required columns and correct data types.
   *
   * @param df DataFrame of flight data
   * @throws IllegalArgumentException if validation fails
   */
  private def validateFlightData(df: DataFrame): Unit = {
    val requiredColumns = Seq("passengerId", "flightId", "from", "to", "date")

    // Check if all required columns exist
    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException("Missing required columns in flight data")

    if (!df.schema("passengerId").dataType.equals(org.apache.spark.sql.types.StringType) ||
      !df.schema("flightId").dataType.equals(org.apache.spark.sql.types.StringType)) {
      throw new IllegalArgumentException(s"Columns 'passengerId' and 'flightId' must be of type String to be cast correctly")
    }

  }

  /**
   * Validates the passenger data DataFrame to ensure it contains required columns and correct data types.
   *
   * @param df DataFrame of passenger data
   * @throws IllegalArgumentException if validation fails
   */
  private def validatePassengerData(df: DataFrame): Unit = {
    val requiredColumns = Seq("passengerId", "firstName", "lastName")

    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException("Missing required columns in passenger data")
    }

    if (!df.schema("passengerId").dataType.equals(org.apache.spark.sql.types.StringType)) {
      throw new IllegalArgumentException(s"Column 'passengerId' must be of type String to be cast correctly")
    }
  }

}
