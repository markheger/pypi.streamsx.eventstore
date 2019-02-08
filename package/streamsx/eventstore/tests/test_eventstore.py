
import streamsx.eventstore as es

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.types as spltypes
import streamsx.spl.toolkit
import streamsx.rest as sr

import unittest
import os


def toolkit_env_var():
    result = True
    try:
        os.environ['STREAMS_EVENTSTORE_TOOLKIT']
    except KeyError: 
        result = False
    return result

def connection_env_var():
    result = True
    try:
        os.environ['EVENTSTORE_CONNECTION']
    except KeyError: 
        result = False
    return result

class TestParams(unittest.TestCase):

    def test_param(self):
        topo = Topology()
        s = topo.source(['Hello World']).as_string()
        es.insert(s, '9.26.150.75:1101', 'sample_db', 'sample_table')
        es.insert(s, '9.26.150.75:1101', 'sample_db', 'sample_table', batch_size=100, max_num_active_batches=5)

##
## Test assumptions
##
## toolkit path is given by STREAMS_EVENTSTORE_TOOLKIT environment var
## connection to event store is given by EVENTSTORE_CONNECTION environment var (IP address and port number needed to connect to IBM Db2 Event Store)
## Expected database name is TESTDB and expected table name is ReviewTable
## val reviewSchema = TableSchema("ReviewTable", 
##       StructType(Array(
##          StructField("userId", LongType, nullable = false),
##          StructField("categoryId", IntegerType, nullable = false),
##          StructField("productName", StringType, nullable = false),
##          StructField("boolfield", BooleanType, nullable = false),
##          StructField("boolfield2", BooleanType, nullable = true),
##          StructField("duration", IntegerType, nullable = false ),
##          StructField("review", StringType, nullable = false))),
##        shardingColumns = Seq("userId"), pkColumns = Seq("userId"))
##
class TestDistributed(unittest.TestCase):


    def setUp(self):
        Tester.setup_distributed(self)


    def _create_stream(self, topo):
        s = topo.source([1,2,3,4,5,6])
        schema=StreamSchema('tuple<int64 userId, int32 categoryId, rstring productName, boolean boolfield, boolean boolfield2, int32 duration, rstring review>').as_tuple()
        return s.map(lambda x : (x,x*2,'Prod'+str(x*2),True,False,0,'x'), schema=schema)

    @unittest.skipIf(toolkit_env_var() == False or connection_env_var() == False, "Missing environment variable.")
    def test_insert(self):
        es_toolkit = os.environ['STREAMS_EVENTSTORE_TOOLKIT']
        es_connection = os.environ['EVENTSTORE_CONNECTION']

        topo = Topology('test_insert_with_result')
        # use toolkit applied with STREAMS_EVENTSTORE_TOOLKIT env var
        streamsx.spl.toolkit.add_toolkit(topo, es_toolkit)
        s = self._create_stream(topo)
        res = es.insert(s, es_connection, 'TESTDB', 'ReviewTable')

        tester = Tester(topo)
        tester.run_for(60)

        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        # Run the test 
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


    @unittest.skipIf(toolkit_env_var() == False or connection_env_var() == False, "Missing environment variable.")
    def test_insert_with_result(self):
        es_toolkit = os.environ['STREAMS_EVENTSTORE_TOOLKIT']
        es_connection = os.environ['EVENTSTORE_CONNECTION']
        topo = Topology('test_insert_with_result')
        # use toolkit applied with STREAMS_EVENTSTORE_TOOLKIT env var
        streamsx.spl.toolkit.add_toolkit(topo, es_toolkit)
        result_schema = StreamSchema('tuple<int64 userId, int32 categoryId, rstring productName, boolean boolfield, boolean boolfield2, int32 duration, rstring review, boolean _Inserted_>')
        beacon = op.Source(topo, "spl.utility::Beacon",
            'tuple<int64 userId, int32 categoryId, rstring productName, boolean boolfield, boolean boolfield2, int32 duration, rstring review>',
            params = {'period': 2.0})
        beacon.userId = beacon.output('(int64)IterationCount()')
        beacon.categoryId = beacon.output('(int32)IterationCount()')
        beacon.productName = beacon.output(spltypes.rstring('ProdValue'))

        res = es.insert(beacon.stream, es_connection, 'TESTDB', 'ReviewTable', batch_size=1, schema=result_schema)
        res.print()
        tester = Tester(topo)
        tester.tuple_count(res, 3, exact=False)
        tester.run_for(60)

        # setup test config
        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='trace')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        # Run the test 
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


