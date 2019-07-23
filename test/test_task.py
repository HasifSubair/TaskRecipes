import os
import shutil
from unittest import TestCase
from unittest.mock import patch
from task import Task, Reader, Writer, TransformRecipes, BeefRecipes
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException

os.chdir(os.path.dirname(__file__))
SPARK = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()


class TestTask(TestCase):
    """
    Task execute() is abstract and cannot be called.
    """

    def test_task_class(self):
        task = Task()
        with self.assertRaises(NotImplementedError):
            task.execute()

    def test_reader_class_one(self):
        """
        Testing the reader class to read test.json file.
        :return:
        """
        reader = Reader("json", "test.json")
        reader.set_spark_session(SPARK)
        self.assertEqual(6, reader.execute().count())

    def test_reader_class_two(self):
        """
        Testing the reader class to read test.json file.
        :return:
        """
        reader = Reader("json", "test1.json")
        reader.set_spark_session(SPARK)
        with self.assertRaises(AnalysisException):
            reader.execute().count()

    def test_writer_class(self):
        """
        Testing the Writer class to write as json to test_result folder.
        """
        reader = Reader("json", "test.json")
        reader.set_spark_session(SPARK)
        data_frame = reader.execute()

        location = "test_result"
        writer = Writer("json", location)
        writer.set_spark_session(SPARK)
        writer.set_data_frame(data_frame)
        writer.execute()
        self.assertEqual(True, len(os.listdir(location)) > 0)
        shutil.rmtree(location)

    def test_transform_recipes(self):
        """
        Reading test json and applying TransformRecipes transformation on it.
        :return:
        """
        reader = Reader("json", "test.json")
        reader.set_spark_session(SPARK)
        data_frame = reader.execute()

        transform = TransformRecipes()
        transform.set_spark_session(SPARK)
        transform.set_data_frame(data_frame)
        df = transform.execute()
        self.assertEqual(2, df.filter("difficulty == 'Hard'").count())
        self.assertEqual(2, df.filter("difficulty == 'Medium'").count())
        self.assertEqual(2, df.filter("difficulty == 'Easy'").count())

    def test_beef_recipes(self):
        """
        Reading test json and applying BeefRecipes transformation on it.
        :return:
        """
        reader = Reader("json", "test.json")
        reader.set_spark_session(SPARK)
        data_frame = reader.execute()

        transform = BeefRecipes()
        transform.set_spark_session(SPARK)
        transform.set_data_frame(data_frame)
        df = transform.execute()
        self.assertEqual(1, df.filter("difficulty == 'Hard'").count())
        self.assertEqual(1, df.filter("difficulty == 'Medium'").count())
        self.assertEqual(1, df.filter("difficulty == 'Easy'").count())
