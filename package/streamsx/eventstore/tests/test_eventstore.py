
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
        os.environ['STREAMSX_EVENTSTORE_TOOLKIT']
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
## Test requirements
##
## toolkit path is given by STREAMSX_EVENTSTORE_TOOLKIT environment var
## connection to event store is given by EVENTSTORE_CONNECTION environment var (IP address and port number needed to connect to IBM Db2 Event Store)
##
## Tables are created with the first tuple, if they do not exist in database.
##
class TestDistributed(unittest.TestCase):


    def setUp(self):
        Tester.setup_distributed(self)
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

    def _create_stream(self, topo):
        s = topo.source([1,2,3,4,5,6])
        schema=StreamSchema('tuple<int32 id, rstring name>').as_tuple()
        return s.map(lambda x : (x,'X'+str(x*2)), schema=schema)

    def test_insert(self):
        es_toolkit = os.environ['STREAMSX_EVENTSTORE_TOOLKIT']
        es_connection = os.environ['EVENTSTORE_CONNECTION']

        topo = Topology('test_insert_with_result')
        # use toolkit applied with STREAMSX_EVENTSTORE_TOOLKIT env var
        streamsx.spl.toolkit.add_toolkit(topo, es_toolkit)
        s = self._create_stream(topo)
        res = es.insert(s, es_connection, 'TESTDB', 'SampleTable', primary_key='id')

        tester = Tester(topo)
        tester.run_for(60)

        # Run the test 
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)


    def test_insert_with_result(self):
        es_toolkit = os.environ['STREAMSX_EVENTSTORE_TOOLKIT']
        es_connection = os.environ['EVENTSTORE_CONNECTION']
        topo = Topology('test_insert_with_result')
        # use toolkit applied with STREAMSX_EVENTSTORE_TOOLKIT env var
        streamsx.spl.toolkit.add_toolkit(topo, es_toolkit)
        result_schema = StreamSchema('tuple<int64 userId, int32 categoryId, rstring productName, boolean boolfield, boolean boolfield2, int32 duration, rstring review, boolean _Inserted_>')
        beacon = op.Source(topo, "spl.utility::Beacon",
            'tuple<int64 userId, int32 categoryId, rstring productName, boolean boolfield, boolean boolfield2, int32 duration, rstring review>',
            params = {'period': 2.0})
        beacon.userId = beacon.output('(int64)IterationCount()')
        beacon.categoryId = beacon.output('(int32)IterationCount()')
        beacon.productName = beacon.output(spltypes.rstring('ProdValue'))

        res = es.insert(beacon.stream, es_connection, 'TESTDB', 'ReviewTable', batch_size=5, primary_key='userId', schema=result_schema)
        res.print()
        tester = Tester(topo)
        tester.tuple_count(res, 3, exact=False)
        tester.run_for(60)

        # Run the test 
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)


