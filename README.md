# Flight Data Analysis Project

  

## Project Overview

  

This project is designed to analyze flight data using Apache Spark and Scala. It processes datasets containing information about passengers and flights, and generates CSV reports answering key questions, such as:

  

- The total number of flights per month.

  

- The top 100 most frequent flyers.

  

- The maximum number of countries a passenger has visited without being in the UK.

  

- Pairs of passengers who have been on more than 3 flights together.

  

- Pairs of passengers who have flown together more than a specified number of times within a certain date range.

  

  

## Table of Contents

  

1. Setup and Dependencies

  

2. Running the Program

  

3. Generated CSV Files

  

4. Testing

  

  

### Setup and Dependencies

  

#### Prerequisites

  

Before running this project, ensure you have the following installed:

  

- JDK 1.8: Make sure that Java 1.8 is installed on your machine.

  

- Scala 2.12.x: The project uses Scala 2.12.10.

  

- Apache Spark 2.4.8: The project is built using Apache Spark 2.4.8.

  

- sbt: sbt (Simple Build Tool) is used for project management and dependency resolution.

- IntelliJ IDEA Community Edition: This project was developed using IntelliJ IDEA Community Edition, a free and open-source IDE for Java, Scala, and more.
  

#### Installing sbt

  

To install sbt, follow the instructions for your operating system:

  

- On macOS: brew install sbt

  

- On Ubuntu: sudo apt-get install sbt

  

- On Windows: Download the installer from sbt's official site.

  

  

#### sbt Configuration

  

Ensure your build.sbt contains the following dependencies:

  

  

```

  

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))

.settings(

name := "SergioPereaDeLaCasa"

)

libraryDependencies ++= Seq(

"org.apache.spark" %% "spark-core" % "2.4.8",

"org.apache.spark" %% "spark-sql" % "2.4.8" ,

"org.scalatest" %% "scalatest" % "3.0.8"

)

  

)

  

```

  

### Running the Program

  
1. Opening the Project in IntelliJ IDEA
Download and install IntelliJ IDEA Community Edition from IntelliJ's official site.

Clone this repository to your local machine using:

```bash
git clone https://github.com/SergioPerea99/FlightData.git
```
Open IntelliJ and select Open Project.

Navigate to the directory where you cloned the repository and select the project folder.

IntelliJ will automatically detect the sbt project and begin indexing and resolving dependencies.

Note: The project is stored in a folder with name "SergioPereaDeLaCasa". You must open this folder in IntelliJ as your main project.


2. Running the Program
To run the program in IntelliJ if the green Run button is not available:

- Run Configuration: Go to Run > Edit Configurations.
- Click the + symbol and select Application.
- Set the following fields:
    - Name: Give your configuration a name, such as FlightDataAnalysis.
    - Main class: Set the main class to Main (i.e., the object that contains the main method).
    - Working directory: Ensure this points to the root of the project directory.
- Click Apply and OK.

### Generated CSV Files

After running the program, the following CSV files will be generated:

  

- question1.csv: Contains the total number of flights per month. Columns: Month, Number of Flights

  

- question2.csv: Contains the names and IDs of the top 100 most frequent flyers. Columns: passengerId, Number of Flights, firstName, lastName

  

- question3.csv: Contains the maximum number of countries a passenger has visited without being in the UK. Columns: passengerId, longest Run

  

- question4.csv: Lists the passengers who have been on more than 3 flights together. Columns: passenger1 ID, passenger2 ID, Number of flights together

  

- question4_extraMarks.csv: Contains pairs of passengers who have flown together more than a specified number of times within a given date range. Columns: passenger1 ID, passenger2 ID, Number of flights together, From, To

  

These files can be found in the root project directory after the program has run successfully.

  
  

### Testing

Unit tests are included for validating the core methods:
  

- groupFlightsByDate: Tests whether flights are correctly grouped by date.

- mostFrequentFlyers: Tests the output for the top 100 frequent flyers.

- longestCountriesWithoutUK: Ensures that the method correctly calculates the maximum number of countries visited consecutively without being in the UK.

- numberFlightsTogether: Validates that passengers who have flown together are correctly identified and counted.