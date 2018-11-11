import io
import hashlib
import logging
import os
import sys
import unittest
try:
    import pandas as pd
except ImportError:
    pd = None

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import knime
del sys.path[0]


class CoreFunctionsTest(unittest.TestCase):
    default_container_input_table_columns = [
        "column-string",
        "column-int",
        "column-double", 
        "column-long",
        "column-boolean",
        "column-localdate",
        "column-localdatetime",
        "column-zoneddatetime",
    ]

    simple_input_data_table_dict = {
        "table-spec": [{"column-int": "int"}, {"b": "string"}],
        "table-data": [[100, "boil"], [0, "freeze"]]
    }

    def templated_test_container_1_input_1_output(
            self,
            input_data_table=None,
            output_as_pandas_dataframes=None
        ):
        with knime.Workflow("tests/knime-workspace/test_simple_container_table_01") as wf:
            if input_data_table is not None:
                wf.data_table_inputs[0] = input_data_table
            if output_as_pandas_dataframes is not None:
                wf.execute(output_as_pandas_dataframes=output_as_pandas_dataframes)
            else:
                wf.execute()
            results = wf.data_table_outputs[:]

        return results


    def test_container_1_input_1_output_dict_input_without_pandas(self):
        results = self.templated_test_container_1_input_1_output(
            input_data_table=self.simple_input_data_table_dict,
            output_as_pandas_dataframes=False,
        )
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], dict))
        returned_table_spec = (list(d)[0] for d in results[0]["table-spec"])
        self.assertEqual(set(returned_table_spec), {"column-int", "b", "computored"})
        returned_computored_values = [
            row[2] for row in results[0]["table-data"]
        ]
        self.assertEqual(returned_computored_values, [4200, 0])


    def test_container_1_input_1_output_dict_input(self):
        results = self.templated_test_container_1_input_1_output(
            input_data_table=self.simple_input_data_table_dict,
        )
        self.assertEqual(len(results), 1)
        if pd is not None:
            self.assertTrue(isinstance(results[0], pd.DataFrame))
        else:
            self.assertTrue(isinstance(results[0], dict))


    def test_container_1_input_1_output_dict_input_with_pandas(self):
        if pd is None:
            self.skipTest("pandas not available")
        results = self.templated_test_container_1_input_1_output(
            input_data_table=self.simple_input_data_table_dict,
            output_as_pandas_dataframes=True,
        )
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], pd.DataFrame))
        self.assertEqual(set(results[0].columns), {"column-int", "b", "computored"})
        self.assertEqual(
            list(results[0]["computored"].values),
            [4200, 0]
        )


    def test_container_1_input_1_output_DataFrame_input(self):
        if pd is None:
            self.skipTest("pandas not available")
        df = pd.DataFrame(
            [[0, "cold"], [15, "warm"], [30, "hot"]],
            columns=["column-int", "description"]
        )
        df["column-int"] = df["column-int"].astype(pd.np.int32)
        results = self.templated_test_container_1_input_1_output(
            input_data_table=df,
        )
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], pd.DataFrame))
        self.assertEqual(
            set(results[0].columns),
            {"column-int", "description", "computored"}
        )
        self.assertEqual(
            list(results[0]["computored"].values),
            [0, 630, 1260]
        )


    def test_container_1_input_1_output_DataFrame_input_no_DataFrame_output(self):
        if pd is None:
            self.skipTest("pandas not available")
        df = pd.DataFrame(
            [[-1, "cold"], [15, "warm"], [30, "hot"]],
            columns=["column-int", "description"]
        )
        df["column-int"] = df["column-int"].astype(pd.np.int32)
        results = self.templated_test_container_1_input_1_output(
            input_data_table=df,
            output_as_pandas_dataframes=False,
        )
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], dict))
        returned_table_spec = (list(d)[0] for d in results[0]["table-spec"])
        self.assertEqual(
            set(returned_table_spec),
            {"column-int", "description", "computored"}
        )
        returned_computored_values = [
            row[2] for row in results[0]["table-data"]
        ]
        self.assertEqual(returned_computored_values, [-42, 630, 1260])


    def test_container_1_input_1_output_no_input_data_without_pandas(self):
        results = self.templated_test_container_1_input_1_output(
            output_as_pandas_dataframes=False,
        )
        self.assertEqual(len(results), 1)
        self.assertTrue(isinstance(results[0], dict))
        returned_table_spec = (list(d)[0] for d in results[0]["table-spec"])
        self.assertEqual(
            set(returned_table_spec),
            set(self.default_container_input_table_columns + ["computored"])
        )


    def test_container_1_input_1_output_no_input_data(self):
        results = self.templated_test_container_1_input_1_output(
            output_as_pandas_dataframes=None,
        )
        self.assertEqual(len(results), 1)
        if pd is not None:
            self.assertTrue(isinstance(results[0], pd.DataFrame))
        else:
            self.assertTrue(isinstance(results[0], dict))


    def test_container_1_input_1_output_no_input_data_with_pandas(self):
        if pd is None:
            self.skipTest("pandas not available")
        results = self.templated_test_container_1_input_1_output(
            output_as_pandas_dataframes=True,
        )
        self.assertEqual(len(results), 1)
        df = results[0]
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEqual(
            set(df.columns),
            set(self.default_container_input_table_columns + ["computored"])
        )
        # Test breadth of coverage of type conversions on return.
        # Fragile test:  assumes 64-bit system used in testing.
        self.assertEqual(
            list(df.dtypes),
            [
                pd.np.dtype("O"),
                pd.np.dtype("int64"),
                pd.np.dtype("float64"),
                pd.np.dtype("int64"),
                pd.np.dtype("bool"),
                pd.np.dtype("O"),
                pd.np.dtype("O"),
                pd.np.dtype("O"),
                pd.np.dtype("int64"),
            ]
        )


    def test_container_1_input_1_output_inappropriate_input_data(self):
        logger = logging.getLogger()

        # Remove current log handlers.
        logging_handlers = logger.handlers[:]
        for lh in logging_handlers:
            logger.removeHandler(lh)

        # Add our own temporary log handler.
        log_buffer = io.StringIO()
        temp_lh = logging.StreamHandler(log_buffer)
        logger.addHandler(temp_lh)

        inappropriate_input_data_table_dict = {
            "table-spec": [{"columnZZZint": "int"}, {"bZZZ": "string"}],
            "table-data": [[101, "boil"], [-10, "freeze"]]
        }

        with self.assertRaises(ChildProcessError):
            results = self.templated_test_container_1_input_1_output(
                input_data_table=inappropriate_input_data_table_dict,
                output_as_pandas_dataframes=False,
            )

        log_buffer.seek(0)
        raw_log_lines = log_buffer.readlines()

        # Restore original log handlers.
        logger.removeHandler(temp_lh)
        for lh in logging_handlers:
            logger.addHandler(lh)

        # Verify relevant available info sent to logging.
        self.assertTrue("captured stdout" in raw_log_lines[0])
        self.assertTrue("captured stderr" in raw_log_lines[1])
        self.assertTrue(len(raw_log_lines[0]) > 25)
        self.assertTrue(len(raw_log_lines[1]) > 25)


    def test_container_1_input_1_output_mismatched_input_datatypes(self):
        # Str in column meant for int and vice versa.
        mismatched_types_input_data_table_dict_1 = {
            "table-spec": [{"column-int": "int"}, {"b": "string"}],
            "table-data": [["boil", 98.7], ["freeze", False]]
        }
        results = self.templated_test_container_1_input_1_output(
            input_data_table=mismatched_types_input_data_table_dict_1,
            output_as_pandas_dataframes=False,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["table-data"]), 2)
        self.assertEqual(len(results[0]["table-data"][0]), 3)
        self.assertEqual(
            results[0]["table-data"],
            [[None, "98.7", None], [None, "false", None]]
        )

        # Float in column meant for int also results in missing values.
        mismatched_types_input_data_table_dict_2 = {
            "table-spec": [{"column-int": "int"}, {"b": "string"}],
            "table-data": [[100.567, "boil"], [0.123, "freeze"]]
        }
        results = self.templated_test_container_1_input_1_output(
            input_data_table=mismatched_types_input_data_table_dict_2,
            output_as_pandas_dataframes=False,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["table-data"]), 2)
        self.assertEqual(len(results[0]["table-data"][0]), 3)
        self.assertEqual(
            results[0]["table-data"],
            [[None, "boil", None], [None, "freeze", None]]
        )


    def test_non_existent_workflow_execution(self):
        with knime.Workflow("tests/knime-workspace/never_gonna_give_you_up") as wf:
            pass  # There was no execute call and so no problem.

        with self.assertRaises(FileNotFoundError):
            with knime.Workflow(
                workspace_path="tests/knime-workspace",
                workflow_path="never_gonna_let_you_down"
            ) as wf:
                # Existence of workflow is only checked in execute().
                wf.execute(output_as_pandas_dataframes=False)
                results = wf.data_table_outputs[:]


    def test_specify_workspace_plus_workflow(self):
        with knime.Workflow(
            workspace_path="tests/knime-workspace",
            workflow_path="test_simple_container_table_01"
        ) as wf:
            wf.execute(output_as_pandas_dataframes=False)
            results = wf.data_table_outputs[:]
        self.assertEqual(len(results), 1)

        with knime.Workflow(
            workspace_path="tests/knime-workspace",
            workflow_path="/test_simple_container_table_01"
        ) as wf:
            wf.execute(output_as_pandas_dataframes=False)
            results = wf.data_table_outputs[:]
        self.assertEqual(len(results), 1)

        with self.assertRaises(FileNotFoundError):
            # Non-existent workflow.
            with knime.Workflow(
                workspace_path="tests/knime-workspace",
                workflow_path="never_gonna_run_around_and_desert_you"
            ) as wf:
                wf.execute(output_as_pandas_dataframes=False)
                results = wf.data_table_outputs[:]

        with self.assertRaises(FileNotFoundError):
            # Non-existent workspace (note the leading slash).
            with knime.Workflow(
                workspace_path="/tests/knime-workspace",
                workflow_path="/test_simple_container_table_01"
            ) as wf:
                wf.execute(output_as_pandas_dataframes=False)
                results = wf.data_table_outputs[:]


    def test_AAAA_nosave_workflow_after_execution_as_default(self):
        with knime.Workflow("tests/knime-workspace/test_simple_container_table_01") as wf:
            wf.execute(output_as_pandas_dataframes=False)
            results = wf.data_table_outputs[:]
            self.assertEqual(wf.data_table_inputs_parameter_names, ("input",))

        with open(wf.path_to_knime_workflow / ".savedWithData") as fp:
            contents_hash = hashlib.md5(fp.read().encode('utf8')).hexdigest()
        self.assertEqual(contents_hash, "ac23b46d2e75be6a9ce5f479104de658")


    def test_zzzz_save_workflow_after_execution(self):
        with knime.Workflow("tests/knime-workspace/test_simple_container_table_01") as wf:
            wf.save_after_execution = True
            wf.execute(output_as_pandas_dataframes=False)
            results = wf.data_table_outputs[:]
            self.assertEqual(wf.data_table_inputs_parameter_names, ("input",))

        with open(wf.path_to_knime_workflow / ".savedWithData") as fp:
            contents_hash = hashlib.md5(fp.read().encode('utf8')).hexdigest()
        self.assertNotEqual(contents_hash, "ac23b46d2e75be6a9ce5f479104de658")



if __name__ == '__main__':
    unittest.main()
