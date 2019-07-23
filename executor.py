"""
Module builds configurable FIFO data pipelines by building and executing tasks in the configured
order. All tasks must extend "pipeline.task.Task" class. The order of the task determined by the
order in which the arguments are passed to the module from the command line.
"""
from sys import argv as args

import datetime
import commons.config as config
import task as pline_task

# Logger instance.
LOGGER = config.get_logger()

# Registered tasks. Its been abstracted here, but this can be loaded from a metadata table in future
# implementations. Key value of the TASKS dictionary denotes the value to be passed as command line
# argument to add the task to the pipeline. Value in TASKS denotes the fully qualified path to the
# corresponding task classes.
TASKS = {"HDFS_READ": "pipeline.task.Reader",
         "HTTP_READ": "pipeline.task.HttpReader",
         "WRITE": "pipeline.task.Writer",
         "PARTITION_WRITE": "pipeline.task.PartitionWriter",
         "FILE_WRITE": "pipeline.task.Writer",
         "RECIPES_TRANSFORM": "pipeline.task.TransformRecipes",
         "BEEF_RECIPE_TRANSFORM": "pipeline.task.BeefRecipes",
         "EMAIL": "pipeline.task.EmailTask",
         "SLACK": "pipeline.task.SlackTask",
         "KAFKA": "pipeline.task.KafkaTask"
         }


def execute_pipeline(task_list):
    """
    Function takes a list of tasks in the order to be executed and executes it. Function also
    passes the data frames returned from previous tasks to the next task in the pipeline.
    :return: None
    """
    data_frame = None
    for task in task_list:
        try:
            # Initial or first task in the pipeline does't won't a data frame to process.
            if data_frame is None:
                data_frame = task.execute()
            else:
                task.set_data_frame(data_frame)
                data_frame = task.execute()
        except Exception as exception:
            LOGGER.exception(
                f"{datetime.datetime.now()} - An exception occurred during the execution of pipeline : {exception}")
            raise


def build_pipeline(tasks, is_spark=True):
    """
    Function takes an argument list and identifies and instantiate individual tasks objects in the
    pipeline in the order it is to be executed.
    Raises ValueError, if the given task is not there in the registered tasks list TASKS.
    :param tasks: list of string
                    A list of task and its parameter in the order to be executed.
    :param is_spark: bool
                    A flag to indicate spark job. Default set to true.
    :return: list
                    A list containing task object in the sequence to be executed.
    """
    # Application name.
    app_name = tasks.pop(0)

    def get_class(kls):
        """
        Function takes the fully qualified class name string as input and returns the corresponding
        Task class
        :param kls: str
                    Fully qualified class name string
        :return: class
                    class
        """
        parts = kls.split('.')
        # Getting the module name
        module_name = ".".join(parts[:-1])
        module = __import__(module_name)
        for comp in parts[1:]:
            module = getattr(module, comp)
        return module

    task_list = []
    # Iterator for tasks
    task_iter = iter(tasks)
    spark_session = None
    if is_spark:
        spark_session = config.get_spark_session(app_name)
    while task_iter:
        try:
            task_instance = None
            # Next task in the list
            next_task = next(task_iter).upper()
            if next_task in TASKS:
                # Getting class corresponding to the string.
                p_task = get_class(TASKS[next_task])
                # PartitionWriter tasks require 3 arguments for file format location and.
                # partition by column.
                # Checking if the class in the list is sub class of type PartitionWriter.
                if issubclass(p_task, pline_task.PartitionWriter):
                    try:
                        task_instance = p_task(file_format=next(task_iter), location=next(task_iter),
                                               partition_by=next(task_iter))
                    except:
                        raise TypeError(f"{task_instance} missing required positional arguments")
                # Reader and Writer tasks require 2 arguments for file format and locatoin.
                # Checking if the class in the list is sub class of type Reader or Writer.
                elif issubclass(p_task, pline_task.Reader) or issubclass(p_task, pline_task.Writer):
                    # Creating an instance of the Task class
                    try:
                        task_instance = p_task(file_format=next(task_iter), location=next(task_iter))
                    except:
                        raise TypeError(f"{task_instance} missing required positional arguments")
                # Transformation tasks doesn't take any arguments.
                elif issubclass(p_task, pline_task.Task):
                    # Creating an instance of the Task class
                    task_instance = p_task()
            else:
                # Raises ValueError, if the given task is not there in the registered tasks list
                # TASKS.
                raise ValueError(f"{next_task} is not a registered task.")
            if is_spark:
                # Injecting a SparkSession object to the class
                task_instance.set_spark_session(spark_session)
            # Adding the task to the task list. Position on the list determines the
            # position of execution.
            task_list.append(task_instance)
        # Stop Iteration at the end of the iterator.
        except StopIteration:
            break
        except Exception as exception:
            LOGGER.exception(f"{datetime.datetime.now()} - An exception occurred while building pipeline {exception}")
            raise
    # Returning task list for execution.
    return task_list


def initiate_pipeline():
    """
    Main function initiate build and execution of the data pipeline.
    :return: NoneType
    """

    # Removing the execution script name from the argument list.
    args.pop(0)
    try:
        # Getting the task list by analyzing the arguments.
        task_list = build_pipeline(args)
        if task_list:
            execute_pipeline(task_list)
    except Exception as exception:
        notify(exception)


def notify(exception):
    """
    Function sends notification to users with the failure report using the configured channels.
    Functions builds a pipeline for the notification and executes it.
    :param exception:
                    The exception to be notified
    :return: None
    """
    app_name = "Notification Task"
    notification_task_list = config.get_notifications()["notify"].split(',')
    notification_task_list.insert(0, app_name)
    task_list = build_pipeline(notification_task_list, False)
    execute_pipeline(task_list)


if __name__ == "__main__":
    # Entry point to the pipeline
    initiate_pipeline()
