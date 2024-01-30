import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToParquetToCassandra {
    public static void main(String[] args) {
        // Set up Spark configuration
        SparkConf conf = new SparkConf().setAppName("AvroToParquetToCassandra").setMaster("local[]");

        // Set AWS credentials directly in the code (not recommended for security reasons)
        //conf.set("access.key", "");
        //conf.set("secret.key", "");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("AvroToParquetToCassandra").getOrCreate();

        // Specify S3 bucket path for input Avro file
        String inputS3Path = "s3a://demo-rsoft-avro-to-parquet/avro-data/sample1.avro";

        // Read Avro file into a DataFrame
        Dataset<Row> avroDataFrame = spark.read().format("avro").load(inputS3Path);

        // Specify S3 bucket path for output Parquet file
        String outputS3Path = "s3a://demo-rsoft-avro-to-parquet/avro-data/parquet-data/sample1.parquet";

        // Write DataFrame to Parquet file in S3
        avroDataFrame.write().parquet(outputS3Path);

        // Specify Cassandra connection parameters
        String cassandraHost = "127.0.0.1";
        String cassandraKeyspace = "avro_to_parquet";
        String cassandraTable = "sample";

        // Write DataFrame to Cassandra
        avroDataFrame.write()
                .format("org.apache.spark.sql.cassandra")
                .option("spark.cassandra.connection.host", cassandraHost)
                .option("keyspace", cassandraKeyspace)
                .option("table", cassandraTable)
                .mode("append")
                .save();

        // Stop Spark context
        spark.stop();
    }
}
