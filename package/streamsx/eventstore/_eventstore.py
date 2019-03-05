# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring

def _add_toolkit_dependency(topo):
    # IMPORTANT: Dependency of this python wrapper to a specific toolkit version
    # This is important when toolkit is not set with streamsx.spl.toolkit.add_toolkit (selecting toolkit from remote build service)
    streamsx.spl.toolkit.add_toolkit_dependency(topo, 'com.ibm.streamsx.eventstore', '[0.1.0,1.0.0)')

    
def insert(stream, connection, database, table, user=None, password=None, config=None, batch_size=None, max_num_active_batches=None, partitioning_key=None, primary_key=None, schema=None, name=None):
    """Inserts tuple into a table using Db2 Event Store Scala API.

    Important: The tuple field types and positions in the IBM Streams schema must match the field names in your IBM Db2 Event Store table schema exactly.

    Creates the table if the table does not exist. Set the ``primary_key`` and/or ``partitioning_key`` in case the table needs to be created.

    Args:
        stream(Stream): Stream of tuples containing the fields to be inserted as a row. Supports ``streamsx.topology.schema.StreamSchema`` (schema for a structured stream) as input. The tuple attribute types and positions in the IBM Streams schema must match the field names in your IBM Db2 Event Store table schema exactly.
        connection(str): The set of IP addresses and port numbers needed to connect to IBM Db2 Event Store.
        database(str): The name of the database, as defined in IBM Db2 Event Store.
        table(str): The name of the table into which you want to insert rows.
        user(str): Name of the IBM Db2 Event Store User in order to connect.
        password(str): Password for the IBM Db2 Event Store User in order to connect.
        config(str): The name of the application configuration. If you specify parameter values in the configuration object, they override the values of ``user`` and ``password`` parameters. Supported properties in the application configuration are: "eventStoreUser" and "eventStorePassword".
        batch_size(int): The number of rows that will be batched in the operator before the batch is inserted into IBM Db2 Event Store by using the batchInsertAsync method. If you do not specify this parameter, the batchSize defaults to the estimated number of rows that could fit into an 8K memory page.
        max_num_active_batches(int): The number of batches that can be filled and inserted asynchronously. The default is 1.        
        partitioning_key(str): Partitioning key for the table. A string of attribute names separated by commas. The partitioning_key parameter is used only, if the table does not yet exist in the IBM Db2 Event Store database.
        primary_key(str): Primary key for the table.  A string of attribute names separated by commas. The order of the attribute names defines the order of entries in the primary key for the IBM Db2 Event Store table. The primary_key parameter is used only, if the table does not yet exist in the IBM Db2 Event Store database.
        schema(StreamSchema): Schema for returned stream. Expects a Boolean attribute called "_Inserted_" in the output stream. This attribute is set to true if the data was successfully inserted and false if the insert failed. Input stream attributes are forwarded to the output stream if present in schema.            
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination
        or
        Output Stream if ``schema`` parameter is specified. This output port is intended to output the information on whether a tuple was successful or not when it was inserted into the database.
    """

    # python wrapper eventstore toolkit dependency
    _add_toolkit_dependency(stream.topology)

    _op = _EventStoreSink(stream, schema, connectionString=connection, databaseName=database, tableName=table, partitioningKey=partitioning_key, primaryKey=primary_key, name=name)

    if batch_size is not None:
        _op.params['batchSize'] = streamsx.spl.types.int32(batch_size)
    if max_num_active_batches is not None:
        _op.params['maxNumActiveBatches'] = streamsx.spl.types.int32(max_num_active_batches)

    if config is not None:
        _op.params['configObject'] = config
    else:
        if user is not None:
            _op.params['eventStoreUser'] = user
        if password is not None:
            _op.params['eventStorePassword'] = password

    if schema is not None:
        return _op.outputs[0]
    else:
        return streamsx.topology.topology.Sink(_op)


class _EventStoreSink(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema, connectionString, databaseName, tableName, batchSize=None, configObject=None, eventStorePassword=None, eventStoreUser=None, frontEndConnectionFlag=None, maxNumActiveBatches=None, nullMapString=None, partitioningKey=None, preserveOrder=None, primaryKey=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.eventstore::EventStoreSink"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if connectionString is not None:
            params['connectionString'] = connectionString
        if databaseName is not None:
            params['databaseName'] = databaseName
        if tableName is not None:
            params['tableName'] = tableName
        if batchSize is not None:
            params['batchSize'] = batchSize
        if configObject is not None:
            params['configObject'] = configObject
        if eventStorePassword is not None:
            params['eventStorePassword'] = eventStorePassword
        if eventStoreUser is not None:
            params['eventStoreUser'] = eventStoreUser
        if frontEndConnectionFlag is not None:
            params['frontEndConnectionFlag'] = frontEndConnectionFlag
        if maxNumActiveBatches is not None:
            params['maxNumActiveBatches'] = maxNumActiveBatches
        if nullMapString is not None:
            params['nullMapString'] = nullMapString
        if partitioningKey is not None:
            params['partitioningKey'] = partitioningKey
        if preserveOrder is not None:
            params['preserveOrder'] = preserveOrder
        if primaryKey is not None:
            params['primaryKey'] = primaryKey

        super(_EventStoreSink, self).__init__(topology,kind,inputs,schema,params,name)



