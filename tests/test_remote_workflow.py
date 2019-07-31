import logging
import os
import sys
import unittest
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import requests
except ImportError:
    # If the requests module is not available, few if any tests will run.
    requests = None

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import knime
del sys.path[0]


@unittest.skipIf(requests is None, "requests unavailable")
class RemoteWorkflowsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.knime_server_urlroot = os.getenv("KNIME_SERVER_URLROOT")
        cls.knime_server_username = os.getenv("KNIME_SERVER_USER")
        cls.knime_server_password = os.getenv("KNIME_SERVER_PASS")
        cls.knime_server_testdir = os.getenv(
            "KNIME_SERVER_TESTDIR",
            f"/Users/{cls.knime_server_username}"
        )


    @unittest.skipIf(pd is None, "pandas unavailable")
    def test_basic_remote_workflow_execution_with_pandas(self):
        simple_input_df = pd.DataFrame(
                        [['blau', 42], ['gelb', -1]],
                        columns=['colors', 'vote']
        )
        workspace_path = self.knime_server_urlroot
        workflow_path = f"{self.knime_server_testdir}/test20190410"
        with knime.Workflow(
            workspace_path=workspace_path,
            workflow_path=workflow_path,
            username=self.knime_server_username,
            password=self.knime_server_password
        ) as wf:
            self.assertEqual(len(wf.data_table_inputs), 1)
            wf.data_table_inputs[:] = [simple_input_df]
            wf.execute(reset=True, timeout_ms=10000)
            self.assertEqual(len(wf.data_table_outputs), 1)
            output_table = wf.data_table_outputs[0]
            self.assertTrue(output_table.equals(simple_input_df))


    def test_basic_remote_workflow_execution_without_pandas(self):
        simple_input_dict = {
            'table-spec': [{'colors': 'string'}, {'vote': 'long'}],
            'table-data': [['blau', 42], ['gelb', -1]]
        }
        workspace_path = self.knime_server_urlroot
        workflow_path = f"{self.knime_server_testdir}/test20190410"
        with knime.Workflow(
            workspace_path=workspace_path,
            workflow_path=workflow_path,
            username=self.knime_server_username,
            password=self.knime_server_password
        ) as wf:
            self.assertEqual(len(wf.data_table_inputs), 1)
            wf.data_table_inputs[:] = [simple_input_dict]
            wf.execute(
                reset=True,
                output_as_pandas_dataframes=False,
                timeout_ms=10000
            )
            self.assertEqual(len(wf.data_table_outputs), 1)
            output_table = wf.data_table_outputs[0]
            self.assertEqual(output_table, simple_input_dict['table-data'])


    def test_obtain_remote_workflow_svg(self):
        workspace_path = self.knime_server_urlroot
        workflow_path = f"{self.knime_server_testdir}/test20190410"
        wf = knime.Workflow(
            workspace_path=workspace_path,
            workflow_path=workflow_path,
            username=self.knime_server_username,
            password=self.knime_server_password
        )
        image = wf._adjust_svg()
        self.assertTrue("clip" in image)


    def test_trigger_timeout_on_remote_workflow_execution(self):
        simple_input_dict = {
            'table-spec': [{'colors': 'string'}, {'rank': 'double'}],
            'table-data': [['blau', 42.7], ['gelb', -1.1]]
        }
        workspace_path = self.knime_server_urlroot
        workflow_path = f"{self.knime_server_testdir}/test20190410"
        with self.assertLogs(level=logging.ERROR):
            with self.assertRaises(RuntimeError):
                with knime.Workflow(
                    workspace_path=workspace_path,
                    workflow_path=workflow_path,
                    username=self.knime_server_username,
                    password=self.knime_server_password
                ) as wf:
                    wf.data_table_inputs[:] = [simple_input_dict]
                    wf.execute(
                        reset=True,
                        timeout_ms=0
                    )
        self.assertEqual(wf._last_status_code, 504)

    def test_no_inputs_remote_workflow_execution(self):
        workflow_url_via_webportal = \
            f"{self.knime_server_urlroot}/#/{self.knime_server_testdir.strip('/')}/quick_ip_address"
        with knime.Workflow(
            workflow_url_via_webportal,  # Verify webportal-style urls work.
            username=self.knime_server_username,
            password=self.knime_server_password
        ) as wf:
            self.assertEqual(len(wf.data_table_inputs), 0)
            wf.execute(timeout_ms=10000)
            output_table = wf.data_table_outputs[0]
        if pd is not None:
            # Verify if pandas is available that it is being used.
            content_type = output_table.iloc[0, 1]
        else:
            # Verify if pandas is not available that we get a list.
            content_type = output_table[0][1]
        self.assertEqual(content_type, "application/json")


if __name__ == '__main__':
    unittest.main()