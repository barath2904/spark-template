from starters.spark_builder import get_spark_session
from starters.spark_logger import Log4j
from pyspark.sql.functions import col, mean, round


class SampleSparkProcessing:
    def __init__(self, logger):
        # app specific custom logging configured spark-defaults.conf
        self.logger = logger

    def read_data(self, spark, data_file):
        self.logger.info("reading data")
        people_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_file)
        return people_df

    def filter_data(self, people_df):
        self.logger.info("filtering data")
        filtered_df = people_df.filter(col('age') < 40)
        return filtered_df

    def process_data(self, filtered_df):
        self.logger.info("processing data")
        processed_df = filtered_df.groupBy(col('department')).agg(round(mean(col('salary'))).alias('average_salary')).\
            orderBy(col('department'))
        return processed_df

    def display_data(self, processed_df):
        self.logger.info("displaying data")
        processed_df.show()


def main():
    spark = get_spark_session()
    logger = Log4j(spark)
    logger.info("Sample Spark program execution started")
    sample_object = SampleSparkProcessing(logger)
    people = sample_object.read_data(spark, "data/people.csv")
    young_people = sample_object.filter_data(people)
    salary_average = sample_object.process_data(young_people)
    sample_object.display_data(salary_average)
    # including input() to hold spark UI
    # input("press enter to close spark")
    logger.info("Sample Spark program execution completed")
    # spark.stop() commented to avoid sporadic py4J network error
    spark.stop()


if __name__ == "__main__":
    main()
