import org.apache.spark.{SparkContext, SparkConf}


object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("./src/main/resources/pulpfiction.txt", 1)

//    val words = file.flatMap(line => line.toLowerCase().replaceAll("\\p{P}"," ").split(" "))
//
//    val fWords = words.filter(w => w.contains("fuck"))
//
//    val ones = fWords.map(word => (word,1))
//
//    val counts = ones.reduceByKey(_ + _).sortByKey()
//
//    val result = counts.collect()

    val result = file
      .flatMap(line => line.toLowerCase().replaceAll("\\p{P}"," ").split(" ")) //cleaning text
      .filter(w => w.contains("fuck")) //looking for the f word
      .map(word => (word,1)) // mapping words
      .reduceByKey(_ + _) //reducing by keys
      .sortByKey() //sort words
      .collect //collecting the result

    result.foreach(println)

    sc.stop()
  }


}
