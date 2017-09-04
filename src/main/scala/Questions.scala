import java.io.{File, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import scala.annotation.tailrec
import org.apache.spark.sql.functions._

case class Address(addressLine1: Option[String],
                   addressLine2: Option[String],
                   cityName: Option[String],
                   stateName: Option[String],
                   postalCode: Option[String],
                   countryCode: Option[String]) {
  def this(row: Row) = this(
    Option(row.getAs("Provider First Line Business Practice Location Address")),
    Option(row.getAs("Provider Second Line Business Practice Location Address")),
    Option(row.getAs("Provider Business Practice Location Address City Name")),
    Option(row.getAs("Provider Business Practice Location Address State Name")),
    Option(row.getAs("Provider Business Practice Location Address Postal Code")),
    Option(row.getAs("Provider Business Practice Location Address Country Code (If outside U.S.)"))
  )
}

case class SizeDistribution(sole: Long, small: Long, medium: Long, large: Long)

object RegularQuestions {

  val PSYCHOLOGIST_PREFIX = "103T"
  val PHYSICIAN_PREFIX = "20"
  val TAXONOMY_CODE_FIELDS = (1 to 15).map("Healthcare Provider Taxonomy Code_" + _)

  /**
    * Returns a column (query constraint) that checks if at least one of the Healthcare Provider Taxonomy Code_* fields
    * starts with the prefix
    */
  def taxonomyStartsWith(df: DataFrame, prefix: String): Column = {
    @tailrec
    def inner(currentIndex: Int, lastIndex: Int, colexpr: Column): Column = {
      if (currentIndex > lastIndex) {
        colexpr
      } else {
        inner(currentIndex + 1, lastIndex, colexpr.or(df("Healthcare Provider Taxonomy Code_" + currentIndex).startsWith(prefix)))
      }
    }
    inner(2, 15, df("Healthcare Provider Taxonomy Code_1").startsWith(prefix))
  }

  /**
    * Finds the (state, count) of the state with the most psychologists
    *
    * @param npiData
    * @return
    */
  def mostPsychologists(npiData: DataFrame): (String, Long) = {
    val isPsychologist = taxonomyStartsWith(npiData, PSYCHOLOGIST_PREFIX)
    val frame = npiData.filter(isPsychologist)
    val count = frame.groupBy("Provider Business Practice Location Address State Name").count()
    val max = count.collect().maxBy(_.getAs[Long]("count"))
    (max.getAs("Provider Business Practice Location Address State Name"), max.getAs("count"))
  }

  /**
    * Returns a DataFrame containing only physicians
    */
  def justPhysicians(npiData: DataFrame): DataFrame = npiData.filter(taxonomyStartsWith(npiData, PHYSICIAN_PREFIX))

  /**
    * Counts the number of size classifications of practices for the provided state names
    */
  def practiceCountSizeDistribution(dataset: Dataset[(Address, Long)], states: Set[String])(implicit spark: SparkSession): RDD[(String, SizeDistribution)] = {
    import spark.implicits._
    // first add filter to include only the states passed in
    val filtered = dataset.filter(practice => practice._1.stateName match {
      case Some(name) => states.contains(name)
      case _ => false
    })
    // promote state to key
    val stateAndPracticeSize = filtered.map { case (address, practiceSize) => (address.stateName.get, practiceSize) }
    val addOp = (z: SizeDistribution, size: Long) => {
      size match {
        case 1 => SizeDistribution(z.sole + 1, z.small, z.medium, z.large)
        case s if s >= 2 && s <= 5 => SizeDistribution(z.sole, z.small + 1, z.medium, z.large)
        case s if s >= 6 && s <= 10 => SizeDistribution(z.sole, z.small, z.medium + 1, z.large)
        case s if s > 10 => SizeDistribution(z.sole, z.small, z.medium, z.large + 1)
      }
    }
    val mergeOp = (s1: SizeDistribution, s2: SizeDistribution) => SizeDistribution(
      s1.sole + s2.sole,
      s1.small + s2.small,
      s1.medium + s2.medium,
      s1.large + s2.large)

    stateAndPracticeSize.rdd.aggregateByKey(SizeDistribution(0, 0, 0, 0))(addOp, mergeOp)
  }

  /**
    * Returns an Dataset containing the count of each taxonomy classification of physicians in each state
    *
    * @param physicians DataFrame containing only physicians
    * @return a Dataset over ((statename, classification), count)
    */
  def statesAndClassifications(physicians: DataFrame, taxonomy: DataFrame)(implicit spark: SparkSession): Dataset[((Option[String], String), Long)] = {
    import spark.implicits._
    val codeToClassification = taxonomy.map(row => (row.getAs[String]("Code"), row.getAs[String]("Classification")))
      .collect().toMap

    val stateAndClassSingles = physicians
      .flatMap(row => {
      val state = Option(row.getAs[String]("Provider Business Practice Location Address State Name"))
      val codes = TAXONOMY_CODE_FIELDS.view.map(field => Option(row.getAs[String](field)))
        .takeWhile(_.isDefined)
        .flatten
      val classifications = codes.map(codeToClassification.apply _).toSet //dedupe

      classifications.map(c => ((state, c), 1)) // this 1 doesn't really do anything
    })
    stateAndClassSingles.groupByKey(_._1).count()
  }

  /**
    * Returns a RDD containing the top taxonomy classification of physician for each state
    *
    * @param physicians DataFrame containing only physicians
    * @return an RDD over ((statename, classification), count)
    */
  def topClassificationsPerState(physicians: DataFrame, taxonomy: DataFrame)(implicit spark: SparkSession): RDD[((Option[String], String), Long)] = {
    val statesAndClass: RDD[((Option[String], String), Long)] = statesAndClassifications(physicians, taxonomy).rdd
    // could use a window function instead, but they're kind of a PITA to use in spark
    // this forces a shuffle. this is expressible via a aggregateBy, but I'm not going to do it
    val groupedByState = statesAndClass.groupBy(_._1._1)
    groupedByState.map { case (_, a) => a.maxBy(_._2) }
  }
}

object Questions {

  def loadCSV(pathToFile: String)(implicit spark: SparkSession): DataFrame =
    spark.read.format("csv").option("header", "true").load(pathToFile)

  def runQuestions(npiData: DataFrame, taxonomy: DataFrame)(implicit spark: SparkSession) = {
    import spark.implicits._
    npiData.persist(StorageLevel.DISK_ONLY)
    // get the state with the most # of psychologists
    val stateWithMostPsychologists @ (state, max) = RegularQuestions.mostPsychologists(npiData)
    // cache a dataframe containing only physicians
    val physiciansOnly = RegularQuestions.justPhysicians(npiData).cache()

    val topClassifications = RegularQuestions.topClassificationsPerState(physiciansOnly, taxonomy).collect()
    val physicianPracticeCounts = physiciansOnly.groupByKey(new Address(_))
      .count()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // get the number of practices with 3 physicians
    val numPracticesWithThree: Long = physicianPracticeCounts.filter(_._2 == 3).count()

    val distributionByState: Seq[(String, SizeDistribution)] = RegularQuestions.practiceCountSizeDistribution(
      physicianPracticeCounts, Set("FL", "TX", "CA", "NY")).collect()

    val bonusResult = BonusQuestion.bonusQuestion(physiciansOnly)

    (stateWithMostPsychologists,
      numPracticesWithThree,
      topClassifications,
      distributionByState,
      bonusResult)
  }

  def main(args: Array[String]) {
    implicit val spark = SparkSession.builder.appName("NPI Questions Application").master("local[*]").getOrCreate()

    val pathToNPIProviders = "./data/npidata_20050523-20170507.csv"
    val pathToDeactivation = "./data/deactiviations_20170509.csv"
    val pathToTaxonomy = "./data/nucc_taxonomy_171.csv"
    val npiData = loadCSV(pathToNPIProviders)
    val deactivationData = loadCSV(pathToDeactivation)
    val taxonomyData = loadCSV(pathToTaxonomy)

    val (q1, q2, q3, q4, bonus) = runQuestions(npiData, taxonomyData)

    ResultWriters.writeAsLiteral(q1, "most_psychologists")
    ResultWriters.writeAsLiteral(q2, "practices_with_3_physicians")
    ResultWriters.writeClassificationBreakdown(q3, "top_classifications_per_state")

    val (statesPerDay, days) = bonus
    ResultWriters.writeDistributionCSV(days, statesPerDay.toSeq, "practices_over_time.csv")
    q3.foreach {
      case ((Some(state), classification), count) =>
      case _ =>
    }

    ResultWriters.writeDistributionCSV(q4, "dist.csv")
  }
}

object BonusQuestion {

  val fm = DateTimeFormat.forPattern("MM/dd/yyyy").withZoneUTC()
  case class NPIChange(state: String, timestamp: Long, delta: Long)

  /**
    * @param sign whether the count (third parameter) is a negative or positive delta e.g. needs to be 1 or -1
    * @param r
    * @return
    */
  def toNPIChange(sign: Int)(r: Row) = {
    NPIChange(r(0).asInstanceOf[String], r(1).asInstanceOf[Long], r(2).asInstanceOf[Long]*sign)
  }

  case class NPIChangeInitial(state: String, delta: Long)
  def toNPIChangeInitial(sign: Int)(r: Row) = {
    NPIChangeInitial(r(0).asInstanceOf[String], r(1).asInstanceOf[Long]*sign)
  }

  /**
    * @param physiciansOnly dataframe containing only physicians
    * @return
    */
  def bonusQuestion(physiciansOnly: DataFrame, startTime: Long = fm.parseDateTime("03/01/2017").getMillis, endTime: Long = fm.parseDateTime("05/01/2017").getMillis)(implicit spark: SparkSession) = {
    import spark.implicits._
    val _physicians: DataFrame = physiciansOnly.filter(
      $"Provider Business Practice Location Address State Name".isin("FL", "TX", "CA", "NY"))

    val dateStrToTs: String => Option[Long] = (s: String) => {
      if (s != null && !s.isEmpty) {
        Some(fm.parseDateTime(s).getMillis)
      } else {
        None
      }
    }

    val tsUdf = udf(dateStrToTs)

    val physicians = _physicians
      .withColumn("tsProviderEnum", tsUdf(_physicians("Provider Enumeration Date")))
      .withColumn("tsNPIDeactivation", tsUdf(_physicians("NPI Deactivation Date")))
      .withColumn("tsNPIReactivation", tsUdf(_physicians("NPI Reactivation Date")))
      .cache()

    val initialActivations = physicians
      .filter(physicians("tsProviderEnum") < startTime)
      .groupBy("Provider Business Practice Location Address State Name")
      .count()
      .map(toNPIChangeInitial(1))

    val initialDeactivations = physicians
        .filter(physicians("tsNPIDeactivation") < startTime)
        .groupBy("Provider Business Practice Location Address State Name")
        .count()
        .map(toNPIChangeInitial(-1))

    val initialReactivations = physicians
      .filter(physicians("tsNPIDeactivation") < startTime)
      .groupBy("Provider Business Practice Location Address State Name")
      .count()
      .map(toNPIChangeInitial(1))

    val unifiedInitial = initialActivations.union(initialDeactivations).union(initialReactivations)
      .groupBy("state").agg(sum("delta").as("delta"))
      .map(toNPIChangeInitial(1))
      .collect()

    val activations = physicians
      .filter((physicians("tsProviderEnum").between(startTime, endTime)))
      .groupBy(physicians("Provider Business Practice Location Address State Name"), physicians("tsProviderEnum"))
      .count()
      .map(toNPIChange(1))

    val deactivations = physicians
      .filter(physicians("tsNPIDeactivation").between(startTime, endTime))
      .groupBy(physicians("Provider Business Practice Location Address State Name"), physicians("tsNPIDeactivation"))
      .count()
      .map(toNPIChange(-1))

    val reactivations = physicians
      .filter((physicians("tsNPIReactivation").between(startTime, endTime)))
      .groupBy(physicians("Provider Business Practice Location Address State Name"), physicians("tsNPIReactivation"))
      .count()
      .map(toNPIChange(1))

    val unified = activations.union(deactivations).union(reactivations)
      .groupBy("state", "timestamp").agg(sum("delta").as("delta"))
      .map(toNPIChange(1))

    // collect and generate csv on the driver
    // first get the initial counts per state as a map
    val statesToInitial: Map[String, Long] = unifiedInitial.map(entry => (entry.state, entry.delta)).toMap

    // get the incremental deltas per state and timestamp

    val dt = DateTimeFormat.forPattern("YYYY-MM-dd").withZoneUTC()
    val unifiedDeltas = unified.map(n => ((n.state, n.timestamp), n.delta))
    val deltaMap: Map[(String, String), Long] = unifiedDeltas.collect()
        .map {case ((a, b), c) => ((a, dt.print(b)), c)}
      .toMap

    val start = new DateTime(startTime)
    val end = new DateTime(endTime)
    val daysCount = Days.daysBetween(start, end).getDays()
    val days = (0 to daysCount).map(i => dt.print(start.plusDays(i)))

    val statesPerDay = statesToInitial.map {case (state, initialCount) => {
      var total = initialCount
      val perDayTotal = for (day <- days) yield {
        val key = (state, day)
        deltaMap.get(key) match {
          case None => total
          case Some(delta) => total += delta; total
        }
      }
      (state, perDayTotal)
    }}
    (statesPerDay, days)
  }
}

object ResultWriters {
  def writeDistributionCSV[A, B](days: Seq[A], dist: Seq[(String, Seq[B])], outfile: String) = {
    val pw = new PrintWriter(new File(outfile))
    pw.println(s"state,${days.mkString(",")}")
    dist.foreach { case (state, seq) =>
      pw.println(s"$state,${seq.mkString(",")}") }
    pw.close()
  }
  def writeDistributionCSV(dist: Seq[(String, SizeDistribution)], outfile: String) = {
    val pw = new PrintWriter(new File(outfile))
    pw.println("state,sole,small,medium,large")
    dist.foreach { case (state, SizeDistribution(sole, small, medium, large)) =>
      pw.println(s"$state,$sole,$small,$medium,$large") }
    pw.close()
  }

  def writeAsLiteral[A](a: A, outfile: String) = {
    val pw = new PrintWriter(new File(outfile))
    pw.println(a.toString)
    pw.close()
  }

  def writeClassificationBreakdown(seq: Seq[((Option[String], String), Long)], outfile: String) = {
    val pw = new PrintWriter(new File(outfile))
    pw.println("state,classification,count")
    seq.foreach {
      case ((Some(state), classification), count) => pw.println(s"$state,$classification,$count")
      case _ =>
    }
    pw.close()
  }
}