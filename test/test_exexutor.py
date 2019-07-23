from unittest import TestCase
from unittest.mock import patch
from task import Task
from commons.config import get_logger
from task import Writer, Reader, PartitionWriter
from executor import build_pipeline, execute_pipeline


class TestExecutor(TestCase):
    LOGGER = get_logger()
    TASKS = {"ONE": "pipeline.test.test_exexutor.NumberOne",
             "TWO": "pipeline.test.test_exexutor.NumberTwo",
             "THREE": "pipeline.test.test_exexutor.NumberThree",
             "FOUR": "pipeline.test.test_exexutor.NumberFour",
             "FIVE": "pipeline.test.test_exexutor.NumberFive",
             "SIX": "pipeline.test.test_exexutor.NumberSix",
             "TASK": "pipeline.task.Task"
             }

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_one(self):
        """
        Running custom tasks and checking the order of execution.
        :return:
        """
        args = "Test_APP ONE TWO THREE".split(" ")
        task_list = build_pipeline(args, False)
        self.assertEqual("Task One", task_list[0].execute())
        self.assertEqual("Task Two", task_list[1].execute())
        self.assertEqual("Task Three", task_list[2].execute())
        self.assertEqual(3, len(task_list))

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_two(self):
        """
        Running a task not in pre configured TASKS list.
        :return:
        """
        args = "Test_APP ONE TWO ABC".split(" ")
        with self.assertRaises(ValueError):
            build_pipeline(args, False)

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_three(self):
        """
        Running a task with missing positional arguments for Writer class.
        :return:
        """
        args = "Test_APP ONE FOUR".split(" ")
        with self.assertRaises(TypeError):
            build_pipeline(args, False)

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_four(self):
        """
        Running a task with missing positional arguments for READER class.
        :return:
        """
        args = "Test_APP ONE FIVE".split(" ")
        with self.assertRaises(TypeError):
            build_pipeline(args, False)

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_five(self):
        """
        Running a task with positional arguments for Writer task class.
        :return:
        """
        args = "Test_APP FOUR A B".split(" ")
        task_list = build_pipeline(args, False)
        self.assertEqual(1, len(task_list))

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_six(self):
        """
        Running a task with positional arguments for Reader task class.
        :return:
        """
        args = "Test_APP FIVE A B".split(" ")
        task_list = build_pipeline(args, False)
        self.assertEqual(1, len(task_list))

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_seven(self):
        """
        Running a task with proper positional arguments for PartitionWriter task class.
        :return:
        """
        args = "Test_APP SIX A B C".split(" ")
        task_list = build_pipeline(args, False)
        self.assertEqual(1, len(task_list))

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_eight(self):
        """
        Running a task with missing positional arguments for READER class.
        :return:
        """
        args = "Test_APP ONE SIX A B".split(" ")
        with self.assertRaises(TypeError):
            build_pipeline(args, False)

    @patch("pipeline.executor.TASKS", TASKS)
    def test_build_pipeline_nine(self):
        """
        Running a task with base Task class. Exception are thrown only while calling the execute().
        :return:
        """
        args = "Test_APP TASK".split(" ")
        task_list = build_pipeline(args, False)
        self.assertEqual(1, len(task_list))

    def test_execute_pipeline_one(self):
        """
        Executing proper subclasses of Task
        :return:
        """
        task_list = [NumberOne(), NumberTwo(), NumberThree()]
        execute_pipeline(task_list)
        self.assertEqual("Task Two", task_list[2].data_frame)

    def test_execute_pipeline_two(self):
        """
        Executing non subclasses of Task without execute method
        :return:
        """
        task_list = [Test()]
        with self.assertRaises(AttributeError):
            execute_pipeline(task_list)

    def test_execute_pipeline_three(self):
        """
        Executing base Task class.
        :return:
        """
        task_list = [Task()]
        with self.assertRaises(NotImplementedError):
            execute_pipeline(task_list)


class NumberOne(Task):
    def execute(self):
        return "Task One"


class NumberTwo(Task):
    def execute(self):
        return "Task Two"


class NumberThree(Task):
    def execute(self):
        return "Task Three"


class NumberFour(Writer):
    def execute(self):
        return "Task Four"


class NumberFive(Reader):
    def execute(self):
        return "Task Five"


class NumberSix(PartitionWriter):
    def execute(self):
        return "Task Six"


class Test():
    pass


class Test1():
    def execute(self):
        return "Test1"
