package scala_proj
import org.apache.spark.sql.types.{ StructType, ArrayType, LongType, StructField, StringType, IntegerType };

object test {
  val pSchema = new StructType()
    .add("_VALUE", StringType)
    .add("_name", StringType)

  val listSchema = new StructType()
    .add("_name", StringType)
    .add("item", ArrayType(pSchema))
    .add("p", (StringType))

  val structureSchema = new StructType()
    .add("_class", StringType)
    .add("_distName", StringType)
    .add("_id", LongType)
    .add("_version", StringType)
    .add("defaults", new StructType()
      .add("_VALUE", StringType)
      .add("_name", StringType))
    .add("list", ArrayType(listSchema))
    .add("p", ArrayType(pSchema))

  val path = "/home/kasun/GSB/samplexml/RC16.21-01-20.gNB.snappy"

  val df = session.read.option("rowTag", "managedObject").
    schema(structureSchema)
    .option("samplingRatio", "0.01")
    .option("org.apache.hadoop.io.compress.CompressionCodec", "snappy")
    .xml(path)

  df.show()
}