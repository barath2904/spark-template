class Log4j:
    def __init__(self, spark):
        root_class = "barath2904.spark.apps"
        spark_config = spark.sparkContext.getConf()
        app_name = spark_config.get("spark.app.name")
        app_id = spark_config.get("spark.app.id")
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(root_class + "." + message_prefix)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
