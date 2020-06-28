from unittest import TestCase
from pyspark.sql import SparkSession
from spark_example import SampleSparkProcessing
from starters.spark_logger import Log4j


class SparkExampleTest(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Testing spark_example").getOrCreate()
        cls.logger = Log4j(cls.spark)
        cls.spark_example = SampleSparkProcessing(cls.logger)

    def test_read_data(self):
        sample_df = SampleSparkProcessing.read_data(self.spark_example, self.spark, "data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 3, "Record count should be 3")

    def test_filter_data(self):
        sample_df = SampleSparkProcessing.read_data(self.spark_example, self.spark, "data/sample.csv")
        filtered_df = SampleSparkProcessing.filter_data(self.spark_example, sample_df)
        result_count = filtered_df.count()
        self.assertEqual(result_count, 3, "Filter count should be 3")

    def test_process_data(self):
        sample_df = SampleSparkProcessing.read_data(self.spark_example, self.spark, "data/sample.csv")
        processed_df = SampleSparkProcessing.process_data(self.spark_example, sample_df)
        result_value = processed_df.collect()[0][1]
        self.assertEqual(result_value, 1500.0, "Average should be 1500.0")

    def test_display_data(self):
        pass

    # commenting to avoid py4J network error
    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()
