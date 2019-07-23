"""
Module manages all application level configuration.
"""
import configparser
import logging
import pyspark.sql.session as session


def get_spark_session(app_name, master='local[*]'):
    """
    Function initiates a SparkSession instance. Master is defaulted to local. Can be extended to
    property driven or removed to be executed with spark-submit.
    :param app_name: str
                    Application name
    :param master: str
                    Spark master URL
    :return: pyspark.sql.session.SparkSession
                    An instance of SparkSession
    """
    return session.SparkSession.Builder().appName(app_name).master(master).getOrCreate()


def read_config(section):
    """
    Function to read the 'config.ini' file. 'config.ini' contains the configurations required for
    the program.
    :param section: str
                    Section of the config.ini file to be read.
    :return: dict
                    Returns a dictionary containing the property names as 'key' and its value
                    as value
    """
    configurations = configparser.ConfigParser()
    configurations.read("config.ini")
    config_dict = {}
    # Iterating through the key in the section.
    for key in configurations[section]:
        config_dict[key] = configurations[section][key]
    return config_dict


def get_logger():
    """
    Function reads logging properties and instantiate the logger instance.
    :return: logging.Logger
                    Returns Logger instance.
    """
    log_properties = read_config("LOG")
    logging.basicConfig(filename=log_properties["location"], level=log_properties["level"])
    return logging


def get_notifications():
    """
    Function reads notification properties from the config.ini file.
    :return: dict
                    Dictionary containing notification properties.
    """
    return read_config("NOTIFICATION")
