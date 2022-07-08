#!/usr/bin/env python
import setuptools

requirements = ["apache-airflow", "rabbitpy", "google-api-python-client"]

setuptools.setup(
        name="custom_component",
        version="1.0.0",
        description="Custom Hooks for [RabbitMQ, YoutubeDataAIP] and Custom Operators for [RabbitMQ,YoutubeDataAPI]",
        author="geup",
        #author_email="",
        install_requires=requirements,
        packages=setuptools.find_packages("src"),
        package_dir={"": "src"},
        #url="",
        #license="",
)

