"""
Module defines tasks that are expected run as part of the pipeline.
"""
import datetime
from pipeline.commons.config import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_extract, when, expr, current_date
from pyspark.sql.session import SparkSession
from pyspark.files import SparkFiles
from pyspark.sql.utils import AnalysisException

LOGGER = get_logger()


class Task:
    """
    An abstract class for the task definition. All tasks must inherit this class and override its
    execute method to run within the pipeline.
    """

    def execute(self):
        """
        An abstract method for executing the task and must be overridden in subclasses.
        Raises NotImplementedError if used without overriding it.
        :return: None
        """
        raise NotImplementedError("Subclasses should override this method.")

    def set_spark_session(self, spark: SparkSession):
        """
        Method for inject SparkSession object for tasks. Pipeline invokes this method and sets the
        SparkSession object.
        :param spark: pyspark.sql.session.SparkSession
                        An instance of SparkSession object.
        :return: None
        """
        self.spark = spark

    def set_data_frame(self, data_frame: DataFrame):
        """
        Method for injecting the DataFrame from previous task to subsequent task within the
        pipeline. The first task in the pipeline will not have any DataFrame. Can be none, it the
        DataFrame need not progressed within the pipeline.
        :param data_frame: pyspark.sql.DataFrame
        :return: None
        """
        self.data_frame = data_frame


class Reader(Task):
    """
    An implementation of abstract Task class for performing the file read operations. A subclass of
    this class must override the execute method for advanced file read operations or for non file
    source systems.
    """

    def __init__(self, file_format, location):
        """
        A constructor for Reader class which initialize the class with values for file_format and
        file location. 'location' can be for a local file with prefix 'file://' followed by path or
        from HDFS with prefix 'hdfs://<HOST_NAME>:<PORT>' followed by path or from 'HTTP/HTTPS'
        with http/https url.
        :param file_format: str
                        File format string.
        :param location: File location
                        File location string.
        """
        self.file_format = file_format
        self.location = location
        Task.__init__(self)

    def execute(self):
        """
        Basic implementation of the abstract execute method of Task class, to read the file from
        specified location and format. Should be overridden for non file based sources and for
        advanced read operations.
        :return: pyspark.sql.DataFrame
                        Returns a Spark's DataFrame with the data from the specified file.
        """
        try:
            return self.spark.read.format(self.file_format).load(self.location)
        except AnalysisException as exp:
            raise


class Writer(Task):
    """
    An implementation of abstract Task class for performing the file write operations. A subclass
    of this class must override the execute method for advanced file write operations or for non
    file source systems.
    """

    def __init__(self, file_format, location):
        """
        A constructor for Writer class which initialize the class with values for file_format and
        file location. 'location' can be for a local file with prefix 'file://' followed by path or
        from HDFS with prefix 'hdfs://<HOST_NAME>:<PORT>' followed by path.
        :param file_format: str
                        File format string.
        :param location: File location
                        File location string.
        """
        self.file_format = file_format
        self.location = location
        Task.__init__(self)

    def execute(self):
        """
        Method to write to a file in specified location and format. Should be overridden for
        partitioning and advanced file write operations. Should be overridden for non file sources
        :return: pyspark.sql.DataFrame
                        Returns a Spark's DataFrame with the data from the specified file.
        """
        try:
            self.data_frame.write.mode('append').format(self.file_format).save(self.location)
            return self.data_frame
        except AnalysisException as exp:
            raise


class PartitionWriter(Writer):
    """
    An extension of Writer class for performing partition based file write operations. A subclass
    of this class must override the execute method for advanced file write operations or for non
    file source systems.
    """

    def __init__(self, file_format, location, partition_by):
        """
        A constructor for PartitionWriter class which initialize the class with values for
        file_format, file location and partition by column. 'location' can be for a local file with
        prefix 'file://' followed by path or from HDFS with prefix 'hdfs://<HOST_NAME>:<PORT>'
        followed by path.
        :param file_format: str
                        File format string.
        :param location: File location
                        File location string.
        """
        self.file_format = file_format
        self.location = location
        self.partition_by = partition_by
        Writer.__init__(self, file_format, location)

    def execute(self):
        """
        Method to write to a file in specified location and format. Should be overridden for
        partitioning and advanced file write operations. Should be overridden for non file sources
        :return: pyspark.sql.DataFrame
                        Returns a Spark's DataFrame with the data from the specified file.
        """
        try:
            self.data_frame.write.partitionBy(self.partition_by).mode('append') \
                .format(self.file_format).save(self.location)
            return self.data_frame
        except AnalysisException as exp:
            raise


class HttpReader(Reader):
    """
    A subclass to the Reader class for reading data from from http/https source system.
    """

    def __init__(self, file_format, location):
        """
        Constructor for HTTPReader class, which initializes the superclass Reader.
        :param file_format: str
                        File format string.
        :param location: File location
                        File location string.
        """
        Reader.__init__(self, file_format, location)

    def execute(self):
        """
        Overrides the execute method of the Reader class to read the file from Http/Https location.
        :return: pyspark.sql.DataFrame
                        Returns a Spark's DataFrame with the data from the specified file.
        """
        try:
            spark_context = self.spark.sparkContext
            spark_context.addFile(self.location)
            return self.spark.read.format(self.file_format) \
                .load(SparkFiles.get(self.location.split('/')[-1]))
        except AnalysisException as exp:
            raise


class TransformRecipes(Task):
    """
    An implementation of Task class, which applies transformation on the recipe data. It classifies
    the recipe based on the amount of time required for prep and cook the recipe.
    """

    def __init__(self):
        """
        Constructor for TransformRecipe class. Initializes the superclass Task.
        """
        Task.__init__(self)

    def execute(self):
        """
        Method overrides the execute method of the abstract Task class. Method classifies the
        recipe based on the difficulty. Difficulty is classified based on the total time required
        for prepping and cooking the food.
        If sum(prep time + cooking time) > 1 Hour is as Hard.
        If sum(prep time + cooking time) > 30 minutes and less than an hour is as Medium
        If sum(prep time + cooking time) < 30 minutes as Easy
        As 'Unknown' for all other cases.
        :return: pyspark.sql.DataFrame
                        Returns a Spark's DataFrame with the transformation for the recipe.
        """
        return self.data_frame \
            .withColumn('cookTimeInMinutes',
                        when(lower(col('cookTime')).endswith('h'),
                             regexp_extract('cookTime', '\d+', 0) * 60)
                        .when(lower(col('cookTime')).endswith('m'),
                              regexp_extract('cookTime', '\d+', 0)).otherwise("")) \
            .withColumn('prepTimeInMinutes',
                        when(lower(col('prepTime')).endswith('h'),
                             regexp_extract('prepTime', '\d+', 0) * 60)
                        .when(lower(col('prepTime')).endswith('m'),
                              regexp_extract('prepTime', '\d+', 0)).otherwise("")) \
            .withColumn('difficulty',
                        when(expr('(cookTimeInMinutes + prepTimeInMinutes) > 60'), "Hard")
                        .when(expr('(cookTimeInMinutes + prepTimeInMinutes) <= 60 and '
                                   '(cookTimeInMinutes + prepTimeInMinutes) >= 30'), 'Medium')
                        .when(expr('(cookTimeInMinutes + prepTimeInMinutes) < 30'), "Easy")
                        .when(expr('cookTimeInMinutes = "" and prepTimeInMinutes > 60'), 'Hard')
                        .when(expr('cookTimeInMinutes = "" and prepTimeInMinutes >= 30 and  prepTimeInMinutes <= 60'),
                              'Medium')
                        .when(expr('cookTimeInMinutes = "" and prepTimeInMinutes < 30'), 'Easy')
                        .when(expr('prepTimeInMinutes = "" and cookTimeInMinutes > 60'), 'Hard')
                        .when(expr('prepTimeInMinutes = "" and cookTimeInMinutes >= 30 and  cookTimeInMinutes <= 60'),
                              'Medium')
                        .when(expr('prepTimeInMinutes = "" and cookTimeInMinutes < 30'), 'Easy')
                        .when(expr('prepTimeInMinutes = "" and cookTimeInMinutes = ""'), 'Unknown')
                        .otherwise("Unknown")) \
            .withColumn('date_of_execution', current_date()) \
            .drop('cookTimeInMinutes') \
            .drop('prepTimeInMinutes')


class BeefRecipes(TransformRecipes):
    """
    An implementation of Writer class, which applies transformation on the recipe data, which has
    'beef' as one of the ingredients. It classifies the recipe based on the amount of time required
     for prep and cook the recipe.
    """

    def __init__(self):
        """
        Constructor for BeefRecipes class. Initializes the superclass TransformRecipes.
        """
        TransformRecipes.__init__(self)

    def execute(self):
        """
        Method classifies the recipes with 'beef' as an ingredient for difficulty. Difficulty is
        classified based on the total time required for prepping and cooking the food. Method
        extends on the super class TransformRecipes classification logic by applying a filter for
        the keyword 'beef' in ingredients column.
        :return:
        """
        return super(BeefRecipes, self).execute().where(lower(col('ingredients')).like("%beef%"))


class EmailTask(Task):
    """
    Abstraction for Email notification class.
    """

    def execute(self):
        """
        Method sends out email notification of error.
        :return:
        """
        return LOGGER.info(f"{datetime.datetime.now()} - Sending EMail to the configured email list")


class SlackTask(Task):
    """
    Abstraction for Slack notification class.
    """

    def execute(self):
        """
        Method sends out slack notification of error.
        :return:
        """
        return LOGGER.info(f"{datetime.datetime.now()} - Sending notification in Slack")


class KafkaTask(Task):
    """
    Abstraction for Slack notification class.
    """

    def execute(self):
        """
        Method sends out message to the dashboard error.
        :return:
        """
        return LOGGER.info(f"{datetime.datetime.now()} - Sending message to Kafka for visualizing")
