import org.apache.spark.sql.SparkSession

object CaseStudy3 {
  case class hvac_cls(Date:String,Time:String,TargetTemp:Int,ActualTemp:Int,System:Int,SystemAge:Int,BuildingId:Int)
  case class building(buildid:Int,buildmgr:String,buildAge:Int,hvacproduct:String,Country:String)

  def main(args: Array[String]): Unit = {

    //Creating spark Session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    println("Spark Session Object created")

    //Loading the hvac.csv file
    val data = spark.sparkContext.textFile("D:\\Lavanya\\HVAC.csv")
    println("HVAC Data->>" + data.count) //data count with header

    val header = data.first()
    val data1 = data.filter(row => row != header)

    println("HVAC Data->>" + data1.count) //data count without header

    //For implicit conversions like converting RDDs and sequences to DataFrames
    import spark.implicits._
    val hvac = data1.map(x=>x.split(",")).map(x => hvac_cls(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt)).toDF()
    hvac.show()
    println("HVAC Dataframe created !")

    hvac.registerTempTable("HVAC") //Registering as temporary table HVAC.
    println("Dataframe Registered as table !")

    //Adding a new column tempchange and to set to 1, if there is a change of greater than +/-5 between actual and target temperature
    val hvac1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")
    hvac1.show()
    hvac1.registerTempTable("HVAC1") //Registering the newly added column table as temp table HVAC1
    println("Data Frame Registered as HVAC1 table !")

    // Loading the second data set building.csv
    val data2 = spark.sparkContext.textFile("D:\\Lavanya\\building.csv")

    //Removing the header line from the building.csv file
    val header1 = data2.first()
    val data3 = data2.filter(row => row != header1)
    println("Header removed from the building data")
    println("Buildings Data->>"+data3.count()) //data count without header

    //converting RDDs and sequences to DataFrames
    val build = data3.map(x=> x.split(",")).map(x => building(x(0).toInt,x(1),x(2).toInt,x(3),x(4))).toDF

    build.show()
    build.registerTempTable("building") //Registering as temporary table building
    println("Buildings data registered as building table")

    //joining the two tables based on building id
    val build1 = spark.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on b.buildid = h.buildingid")
    build1.show()

    //Selecting temperature and country column from joined table.
    val tempCountry = build1.map(x => (new Integer(x(7).toString),x(8).toString))
    tempCountry.show()

    //Filtering the values to check the rows where tempchange is 1
    val tempCountryOnes = tempCountry.filter(x=> {if(x._1==1) true else false})
    tempCountryOnes.show()

    tempCountryOnes.groupBy("_2").count.show //counting the number of occurence for each country.

  }
}
