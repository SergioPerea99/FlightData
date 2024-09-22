import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class FlightProcessingTest extends FunSuite {

  val spark = SparkSession.builder().appName("Flight Data Test").master("local").getOrCreate()

  import spark.implicits._

  //Question 1
  test("groupFlightsByDate should count total flights per month") {
    val flightData = Seq(
      FlightData(1, 1, "sp", "fr", "2021-01-15"),
      FlightData(2, 2, "sp", "uk", "2021-01-20"),
      FlightData(3, 3, "sp", "it", "2021-02-10")
    ).toDS()

    Main.groupFlightsByDate(flightData, month(col("date")), "Month", spark, csvPath = "../test/question1.csv")
    val result = spark.read.option("header", "true").option("delimiter", ";").csv("../test/question1.csv")
    assert(result.count() == 2) // 2 months of data
  }

  //Question 2
  test("mostFrequentFlyers should find the top 100 flyers") {

    val flightData = Seq(
      FlightData(1, 1, "sp", "fr", "2021-01-15"),
      FlightData(2, 2, "sp", "uk", "2021-01-20"),
      FlightData(3, 3, "sp", "it", "2021-02-10"),
      FlightData(1, 4, "sp", "uk", "2021-02-15"),
      FlightData(1, 5, "sp", "fr", "2021-03-15"),
      FlightData(1, 6, "sp", "it", "2021-04-15")
    ).toDS()

    val passengerData = Seq(
      Passenger(1, "John", "Doe"),
      Passenger(2, "Jane","Smith")
    ).toDS()

    // Run the method to be tested
    Main.mostFrequentFlyers(passengerData, flightData, spark, csvPath = "../test/question2.csv")

    // Load the CSV and verify the number of rows
    val result = spark.read.option("header", "true").option("delimiter", ";").csv("../test/question2.csv")
    assert(result.filter($"passengerId" === 1).count() == 1)
    assert(result.filter($"Number of Flights" === 4).count() == 1)
  }


  //Question 3
  test("Test longestCountriesWithoutUK logic") {
    val flightData = Seq(
      FlightData(1, 101, "uk", "fr", "2024-01-01"),
      FlightData(1, 102, "fr", "gr", "2024-01-02"),
      FlightData(1, 103, "gr", "sp", "2024-01-03"),
      FlightData(1, 104, "sp", "gr", "2024-01-04")
    ).toDS()

    Main.longestCountriesWithoutUK(flightData, spark, "../test/question3.csv")
    val result = spark.read.option("header", "true").option("delimiter", ";").csv("../test/question3.csv")
    assert(result.filter($"Passenger ID" === 1).count() == 1) //PassengerId: 1
    assert(result.filter($"Longest Run" === 3).count() == 1) //Consecutive countries without being in UK: 3

  }

  //Question 4
  test("numberFlightsTogether should find passengers who flew together more than 3 times") {

    val flightData = Seq(
      FlightData(1, 1, "sp", "fr", "2021-01-15"),
      FlightData(2, 1, "sp", "fr", "2021-01-15"),
      FlightData(1, 2, "sp", "uk", "2021-01-20"),
      FlightData(2, 2, "sp", "uk", "2021-01-20"),
      FlightData(1, 3, "sp", "it", "2021-02-10"),
      FlightData(2, 3, "sp", "it", "2021-02-10"),
      FlightData(1, 4, "sp", "it", "2021-02-10"),
      FlightData(2, 4, "sp", "it", "2021-02-10"),
      FlightData(3, 1, "sp", "fr", "2021-01-15"),
      FlightData(3, 2, "sp", "uk", "2021-01-20"),
      FlightData(3, 10, "sp", "it", "2021-04-15")
    ).toDS()


    Main.numberFlightsTogether(flightData, 3, spark, csvPath = "../test/question4.csv")
    val result = spark.read.option("header", "true").option("delimiter", ";").csv("../test/question4.csv")
    assert(result.count() == 1) // Only one pair should have flown together more than 3 times

  }


  //Question 4. Extra Marks.
  test("flownTogether should find passengers who flew together more than N times within the range (from,to) ") {

    val flightData = Seq(
      FlightData(1, 1, "sp", "fr", "2021-01-15"),
      FlightData(2, 1, "sp", "fr", "2021-01-15"),
      FlightData(1, 2, "sp", "uk", "2021-01-20"),
      FlightData(2, 2, "sp", "uk", "2021-01-20"),
      FlightData(1, 3, "sp", "it", "2021-01-30"),
      FlightData(2, 3, "sp", "it", "2021-01-30"),
      FlightData(1, 4, "sp", "it", "2021-01-31"),
      FlightData(2, 4, "sp", "it", "2021-01-31"),
      FlightData(3, 1, "sp", "fr", "2021-01-15"),
      FlightData(3, 2, "sp", "uk", "2021-01-20"),
      FlightData(3, 10, "sp", "it", "2021-04-15")
    ).toDS()

    val csvPath = "../test/question4_2021-01-01_2021-02-01.csv"
    Main.flownTogether(atLeastNTimes = 3, from = "2021-01-01", to = "2021-02-01", df_flights = flightData, sparkSession = spark, csvPath = csvPath)
    val result = spark.read.option("header", "true").option("delimiter", ";").csv(csvPath)
    assert(result.count() == 1) // Only one pair should have flown together more than 3 times

  }


}

