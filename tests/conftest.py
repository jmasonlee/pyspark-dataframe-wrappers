#   ---------------------------------------------------------------------------------
#   Copyright (c) Microsoft Corporation. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   ---------------------------------------------------------------------------------
"""
This is a configuration file for pytest containing customizations and fixtures.

In VSCode, Code Coverage is recorded in config.xml. Delete this file to reset reporting.
"""

from __future__ import annotations
import os
import sys

from _pytest.fixtures import FixtureRequest
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pytest
from _pytest.nodes import Item


def pytest_collection_modifyitems(items: list[Item]):
    for item in items:
        if "spark" in item.nodeid:
            item.add_marker(pytest.mark.spark)
        elif "_int_" in item.nodeid:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def spark(request: FixtureRequest):
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    conf = (SparkConf().set("spark.default.parallelism", "1")
        .setMaster("local")
        .setAppName("sample_pyspark_testing_starter"))

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    request.addfinalizer(lambda: spark.stop())
    return spark

