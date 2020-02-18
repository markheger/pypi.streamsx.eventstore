
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
import glob
import shutil
import uuid
from tempfile import gettempdir


class TestParams(unittest.TestCase):

    def test_param(self):
        topo = Topology()
        s = topo.source(['Hello World']).as_string()
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table')
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5)
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5, front_end_connection_flag=True)
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5, plugin_flag=True)
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5, plugin_flag=False)
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5, plugin_flag='false')
        es.insert(s, connection='9.26.150.75:1101', database='sample_db', table='sample_table', batch_size=100, max_num_active_batches=5, ssl_connection=False)

    def test_update_toolkit(self):
        topo = Topology()
        url = None
        # download event store toolkit from GitHub
        eventstore_toolkit = es.download_toolkit(url)
        # add event store toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topo, eventstore_toolkit)

    def test_get_service_details(self):
        topo = Topology()
        es_cfg = None
        self.assertRaises(ValueError, es.get_service_details, es_cfg)


##
## Test requirements
##
## toolkit path is given by STREAMSX_EVENTSTORE_TOOLKIT environment var
##
class TestDistributed(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.es_toolkit = os.environ['STREAMSX_EVENTSTORE_TOOLKIT']
        self.jdbc_toolkit_home = os.environ["STREAMS_JDBC_TOOLKIT"]

    def setUp(self):
        Tester.setup_distributed(self)
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def _create_stream(self, topo):
        s = topo.source([1,2,3,4,5,6])
        schema=StreamSchema('tuple<int32 id, rstring name>').as_tuple()
        return s.map(lambda x : (x,'X'+str(x*2)), schema=schema)

    def test_insert(self):
        print ('\n---------'+str(self))
        name = 'test_insert'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.es_toolkit)
        s = self._create_stream(topo)
        res = es.insert(s, config='eventstore', table='SampleTablePy', primary_key='id', ssl_connection=False, plugin_flag=False)      
        # build only
        self._build_only(name, topo)

    def test_statement(self):
        print ('\n---------'+str(self))
        name = 'test_statement'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)
        s = topo.source(['DROP TABLE STR_SAMPLE']).as_string()
        res_sql = s.map(es.SQLStatement(credentials='eventstore'), schema=CommonSchema.String)
        res_sql.print()
        # build only
        self._build_only(name, topo)

    def test_insert_composite(self):
        print ('\n---------'+str(self))
        name = 'test_insert_composite'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.es_toolkit)
        s = self._create_stream(topo)
        s.for_each(es.Insert(config='eventstore', table='SampleTablePy', primary_key='id', ssl_connection=False, plugin_flag=False))
        # build only
        self._build_only(name, topo)


class TestDownloadToolkit(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        # delete downloaded *.tgz (should be deleted in _download_toolkit(...)
        for f in glob.glob(gettempdir() + '/toolkit-[0-9]*.tgz'):
            try:
                os.remove(f)
                print ('file removed: ' + f)
            except:
                print('Error deleting file: ', f)
        # delete unpacked toolkits
        for d in glob.glob(gettempdir() + '/pypi.streamsx.eventstore.tests-*'):
            if os.path.isdir(d):
                shutil.rmtree(d)
        for d in glob.glob(gettempdir() + '/com.ibm.streamsx.eventstore'):
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_download_latest(self):
        topology = Topology()
        location = es.download_toolkit()
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_with_url(self):
        topology = Topology()
        url = 'https://github.com/IBMStreams/streamsx.eventstore/releases/download/v2.2.0/streamsx.eventstore.toolkits-2.2.0-20190731-0640.tgz'
        location = es.download_toolkit(url=url)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_latest_with_target_dir(self):
        topology = Topology()
        target_dir = 'pypi.streamsx.eventstore.tests-' + str(uuid.uuid4()) + '/eventstore-toolkit'
        location = es.download_toolkit(target_dir=target_dir)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_with_url_and_target_dir(self):
        topology = Topology()
        target_dir = 'pypi.streamsx.eventstore.tests-' + str(uuid.uuid4()) + '/eventstore-toolkit'
        url = 'https://github.com/IBMStreams/streamsx.eventstore/releases/download/v2.2.0/streamsx.eventstore.toolkits-2.2.0-20190731-0640.tgz'
        location = es.download_toolkit(url=url, target_dir=target_dir)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

