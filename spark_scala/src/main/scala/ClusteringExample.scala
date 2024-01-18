import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel, VectorAssembler}
import org.apache.spark.sql.SparkSession

object ClusteringExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("k-means").setMaster("local[2]")
    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    // 读取数据
    val inputfile = "spark_scala/src/main/resources/data/consumption_data.xls"
    val outputfile = "spark_scala/src/main/resources/data/data_type.xls"
    val data = spark.read.format("com.crealytics.spark.excel")
      .option("path", inputfile)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("header", "true")
      .load()

    // 特征标准化
    val featureCols = data.columns.filter(_ != "Id")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val dataWithFeatures = assembler.transform(data)

    val scalerModel: StandardScalerModel = scaler.fit(dataWithFeatures)
    val scaledData = scalerModel.transform(dataWithFeatures)

    // 使用KMeans进行聚类
    val k = 3
    val iteration = 500
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIter(iteration)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("cluster")

    val model = kmeans.fit(scaledData)

    // 统计各个类别的数目
    val predictions = model.transform(scaledData)
    predictions.groupBy("cluster").count().show()

    // 保存聚类结果
    predictions.select("Id", "cluster").write
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .mode("overwrite")
      .save(outputfile)

    spark.stop()
  }
}
