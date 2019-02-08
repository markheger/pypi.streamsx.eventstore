# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides a function to insert IBM Streams tuple data into a table in IBM Db2 Event Store.
IBM Db2 Event Store is an in-memory database designed to rapidly ingest and analyze streamed data in event-driven applications. It provides the fabric for fast data with its ability to process massive volume of events in real-time, coupled with optimization for streamed data performance for advanced analytics and actionable insights.

This package exposes the `com.ibm.streamsx.eventstore` toolkit as Python methods.

* `IBM Streaming Analytics <https://www.ibm.com/cloud/streaming-analytics>`_
* `IBM Db2 Event Store <https://www.ibm.com/products/db2-event-store>`_


"""

__version__='0.1.0'

__all__ = ['insert']
from streamsx.eventstore._eventstore import insert
