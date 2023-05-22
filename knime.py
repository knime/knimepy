"""Utilities for working with KNIME workflows and data.

Via the Workflow class, a KNIME Workflow can be run via KNIME's batch
executor (requires KNIME be installed on the local system).  Inputs
to the "Container Input (Table)" nodes in a KNIME Workflow can be
supplied as either Python dicts or pandas DataFrames.  Likewise,
outputs captured from "Container Output (Table)" nodes are provided
back as either Python dicts or pandas DataFrames.

TODOs:
    * add handling for setting of workflow variables
      - only via Container Input (Variable) nodes?
      - also via batch executor variable settings?
    * add handling for Container Input/Output (JSON) nodes
    * support remote workflows

"""


import json
import re
import xml.etree.ElementTree as ElementTree
from locale import getpreferredencoding
from pathlib import Path, PurePosixPath
import tempfile
import subprocess
import shlex
import warnings
import logging
import os
from urllib.parse import urlparse
try:
    import requests
except ImportError:
    # RemoteWorkflow capabilities will be unavailable
    pass
try:
    import pandas
except ImportError:
    # Optional support for returning pandas DataFrames will be unavailable
    pandas = None


__author__ = "Appliomics, LLC"
__copyright__ = "Copyright 2018-2020, KNIME AG"
__credits__ = [ "Davin Potts", "Greg Landrum" ]
__version__ = "0.11.6"


__all__ = [ "Workflow", "LocalWorkflow", "RemoteWorkflow", "executable_path" ]


if os.name == "nt":
    executable_path = os.getenv("KNIME_EXEC", r"C:\Program Files\KNIME\knime.exe")
else:
    executable_path = os.getenv("KNIME_EXEC", "/opt/local/knime_4.3.0/knime")


KEYPHRASE_LOCKED = b"Workflow is locked by another KNIME instance"


def find_service_table_node_dirnames(path_to_knime_workflow):
    """Returns a tuple containing the unique directory names of the Container
    Input and Output (Table) nodes employed by the KNIME workflow in the
    specified path on disk.  The output tuple contains two lists, the first
    lists Container Input (Table) node directory names and the second lists
    Container Output (Table) nodes."""

    def get_xml_encoding(file_path):
        """Returns a string containing the proper encoding for input XML filepath.
        If no encoding information exists in the XML declaration line,
        then the function returns machine-preferred encoding as also used by Python open()."""

        pattern = re.compile(rb'encoding=([\"\'])(.*)\1')
        # encoding="<matches anything inside>" wrapped in proper quotes, if exists
        with open(file_path, mode='rb') as file_bytes:
            header = file_bytes.readline()
            # Assuming encoding information is on the first line following the XML standard
            # Reference: https://www.w3.org/TR/REC-xml/#charencoding
        try:
            capture_encoding = pattern.search(header).group(2)
        except AttributeError:
            return getpreferredencoding()
            # getpreferredencoding() is by default what open() invokes to get encoding information
        proper_encoding = bytes.decode(capture_encoding)
        return proper_encoding

    input_service_table_node_dirnames = []
    output_service_table_node_dirnames = []

    for settings_filepath in Path(path_to_knime_workflow).glob("*/settings.xml"):
        xml_encoding = get_xml_encoding(settings_filepath)
        with settings_filepath.open(encoding=xml_encoding) as fh:
            for line in fh:
                if "ContainerTableInputNodeFactory" in line:
                    *extra, dirname, _settings_xml = settings_filepath.parts
                    input_service_table_node_dirnames.append(dirname)
                    break
                elif "ContainerTableOutputNodeFactory" in line:
                    *extra, dirname, _settings_xml = settings_filepath.parts
                    output_service_table_node_dirnames.append(dirname)
                    break

    return input_service_table_node_dirnames, output_service_table_node_dirnames


def find_service_table_input_node_parameter_name(
    path_to_knime_workflow,
    unique_node_dirname
):
    """Returns the unique-to-the-workflow parameter name setting from
    the specified Container Input (Table) Node."""
    tree = ElementTree.parse(
        Path(path_to_knime_workflow, unique_node_dirname, "settings.xml")
    )
    top_config = tree.getroot()

    parameter_name = None
    for config in top_config:
        if config.attrib.get("key") == "model":
            for entry in config:
                if entry.attrib.get("key") == "parameterName":
                    parameter_name = entry.attrib.get("value")
                    break
            break

    return parameter_name


def find_node_id(path_to_knime_workflow, unique_node_dirname):
    """Returns the unique node id for a KNIME node identified by its
    unique directory name on disk.  For example, a Container Input (Table)
    Node appearing in a KNIME workflow is given a unique directory name
    on disk such as "Container Input _Table_ (#42)"."""

    tree = ElementTree.parse(Path(path_to_knime_workflow, "workflow.knime"))
    top_config = tree.getroot()

    for entry in top_config:
        if entry.attrib.get("key") == "nodes" and entry.tag.endswith("config"):
            # Attempt to infer the namespace being used rather than require
            # one particular version of the KNIME XML namespace.
            config_tag_name = entry.tag
            break
    else:
        raise IndexError("nodes config XML tag not found")

    target_value = str(PurePosixPath(unique_node_dirname, "settings.xml"))
    for node_config in entry.iterfind(config_tag_name):
        for sub_tag in node_config:
            if sub_tag.attrib.get("key") == "id":
                node_id = int(sub_tag.attrib["value"])
            if sub_tag.attrib.get("value") == target_value:
                break
        else:
            node_id = None
        if node_id is not None:
            break

    return node_id


map_numpy_to_knime_type = (
    ('float', 'double'),
    ('int64', 'long'),
    ('long', 'long'),
    ('int', 'int'),
    ('bool', 'boolean')
)

def pandas_type_mapper(pandas_dtype):
    "Converts a pandas dtype to a comparable KNIME data type (as a string)."
    key = str(pandas_dtype)
    for np_type, knime_type in map_numpy_to_knime_type:
        if np_type in key:
            return knime_type
    return 'string'


def convert_dataframe_to_knime_friendly_dict(df):
    """Produces a dict from a pandas DataFrame-like input that is structured
    to be friendly to KNIME when converted to then consumed as json.

    Known issue:  Uses pandas.DataFrame.to_dict(orient="split") which will
    make use of the values array rather than individual Series and as such
    may cause "upcasting" of certain columns' data.  An example of this
    would be a DataFrame containing only int64 and float64 columns.  The
    output from to_dict(orient="split") will be a list of lists of float.
    If upcasting is not possible, say in the same example a column is added
    containing str's, then to_dict() will treat all as Python objects and
    thus output a list of lists of int, float, str as appropriate.
    """

    try:
        proto_table_spec = [
            (column_name, pandas_type_mapper(dtype))
            for column_name, dtype in df.dtypes.items()
        ]

        # If an encountered column's dtype does not readily map to a KNIME
        # data type, it will be conveyed to KNIME as a 'string'.  To ensure
        # proper conversion to json, a copy of the original DataFrame is
        # created, containing str values in otherwise problematic columns.
        df2 = df.copy()
        for column_name, knime_type in proto_table_spec:
            if knime_type == "string":
                df2[column_name] = df2[column_name].apply(str)

        if df2.isna().any().any():
            # If any NaN values exist, ensure they convert to null in final json.
            cleaned_table_data = json.loads(df2.to_json(orient="values"))
        else:
            cleaned_table_data = df2.to_dict(orient="split")["data"]
        data = {
            "table-spec": [ {c: t} for c, t in proto_table_spec ],
            "table-data": cleaned_table_data,
        }

    except AttributeError:
        data = df  # Presume not really a pandas.DataFrame; pass it along.

    return data


def run_workflow_using_multiple_service_tables(
        input_datas,
        path_to_knime_executable,
        path_to_knime_workflow,
        input_service_table_node_ids,
        output_service_table_node_ids,
        *,
        save_after_execution=False,
        live_passthru_stdout_stderr=False,
        output_as_pandas_dataframes=True if pandas else False,
        input_json_filename_pattern="input_%d.json",
        output_json_filename_pattern="output_%d.json",
    ):
    """Executes the requested KNIME workflow, feeding the supplied data
    to the Container Input (Table) nodes in that workflow and returning the
    output from the workflow's Container Output (Table) nodes."""

    abspath_to_knime_workflow = Path(path_to_knime_workflow).resolve(strict=True)
    if not Path(path_to_knime_executable).exists():
        raise ValueError(f"Executable not found: {path_to_knime_executable}")

    with tempfile.TemporaryDirectory() as temp_dir:
        logging.debug(f"using temp dir: {temp_dir}")

        option_flags_input_service_table_nodes = []
        for node_id, data in zip(input_service_table_node_ids, input_datas):
            if data is None:
                warnings.warn(f'No input set for node_id={node_id}', UserWarning)
                continue

            input_json_filename = input_json_filename_pattern % node_id
            input_json_filepath = Path(temp_dir, input_json_filename)

            # Support pandas DataFrame-like inputs.
            data = convert_dataframe_to_knime_friendly_dict(data)

            with open(input_json_filepath, "w") as input_json_fh:
                json.dump(data, input_json_fh)

            option_flags_input_service_table_nodes.append(
                f'-option={node_id},inputPathOrUrl,"{input_json_filepath}",String'
            )

        option_flags_output_service_table_nodes = []
        expected_output_json_files = []
        for node_id in output_service_table_node_ids:
            output_json_filename = output_json_filename_pattern % node_id
            output_json_filepath = Path(temp_dir, output_json_filename)

            option_flags_input_service_table_nodes.append(
                f'-option={node_id},outputPathOrUrl,"{output_json_filepath}",String',
            )
            expected_output_json_files.append(output_json_filepath)

        data_dir = Path(temp_dir, "knime_data")

        # shlex.quote handles executable paths containing spaces, etc.
        # On Windows, cmd shell requires double-quotes, hence replace()
        shell_command = " ".join([
            shlex.quote(path_to_knime_executable).replace("'", '"'),
            "-nosplash",
            "-debug",
            "--launcher.suppressErrors",
            "-application org.knime.product.KNIME_BATCH_APPLICATION",
            f"-data {data_dir}" if not save_after_execution else "",
            "-nosave" if not save_after_execution else "",
            f'-workflowDir="{abspath_to_knime_workflow}"',
            " ".join(option_flags_input_service_table_nodes),
            " ".join(option_flags_output_service_table_nodes),
        ])
        logging.info(f"knime invocation: {shell_command}")

        result = subprocess.run(
            shell_command,
            shell=True if os.name != "nt" else False,
            stdout=subprocess.PIPE if not live_passthru_stdout_stderr else None,
            stderr=subprocess.PIPE if not live_passthru_stdout_stderr else None,
        )
        logging.info(f"exit code from KNIME execution: {result.returncode}")

        knime_outputs = []
        try:
            for output_json_filepath in expected_output_json_files:
                with open(output_json_filepath) as output_json_fh:
                    single_node_knime_output = json.load(output_json_fh)
                knime_outputs.append(single_node_knime_output)
        except FileNotFoundError:
            if result.stderr and KEYPHRASE_LOCKED in result.stderr:
                raise ChildProcessError(KEYPHRASE_LOCKED.decode('utf8'))

            try:
                logging.error(f"captured stdout: {result.stdout.decode('utf8')}")
                logging.error(f"captured stderr: {result.stderr.decode('utf8')}")
            except:
                logging.error(f"captured stdout: {result.stdout}")
                logging.error(f"captured stderr: {result.stderr}")
            raise ChildProcessError("Output from KNIME not found")

        if output_as_pandas_dataframes:
            try:
                for i, output in enumerate(knime_outputs):
                    df_columns = list(
                        k for d in output['table-spec']
                        for k, v in d.items()
                    )
                    knime_outputs[i] = pandas.DataFrame(
                        output['table-data'],
                        columns=df_columns
                    )
            except ImportError:
                logging.warning("requested output as DataFrame not possible")
            except Exception as e:
                logging.error("error while converting KNIME output to DataFrame")
                raise e

        if result.returncode != 0:
            logging.warning("Return code from KNIME execution was non-zero")
            logging.warning(f"captured stdout: {result.stdout}")
            logging.warning(f"captured stderr: {result.stderr}")

    return knime_outputs


class Workflow:
    "Factory class for working with KNIME workflows; not for subclassing."

    def __new__(cls, workflow_path, *, workspace_path=None, **kwargs):
        if (
                workflow_path.startswith(r"https://") or
                workflow_path.startswith(r"http://") or
                (
                    workspace_path is not None and (
                        workspace_path.startswith(r"https://") or
                        workspace_path.startswith(r"http://")
                    )
                )
           ):
            # URL for workflow on KNIME Server is handled by RemoteWorkflow
            cls = RemoteWorkflow
        else:
            # Local filesystem workflow is handled by LocalWorkflow
            cls = LocalWorkflow
        return cls(workflow_path, workspace_path=workspace_path, **kwargs)


class LocalWorkflow:
    """Enables reading and executing of local KNIME workflows.

    Create a LocalWorkflow by specifying the path to the KNIME workflow's
    location on disk.  The `workflow_path` may be a relative or absolute
    path.  Alternatively, a `workspace_path` that points to a KNIME
    workspace's location on disk may be provided so that the supplied
    `workflow_path` can instead be relative to the workspace's location.
    """

    __slots__ = ("_data_table_inputs", "_data_table_outputs",
            "_service_table_input_nodes", "_service_table_output_nodes",
            "save_after_execution",
            "path_to_knime_workflow", "_input_ids", "_output_ids")

    def __init__(self, workflow_path, *, workspace_path=None, save_after_execution=False):
        if workspace_path is not None:
            try:
                workflow_path_as_path = Path(workflow_path).relative_to("/")
            except ValueError:
                workflow_path_as_path = Path(workflow_path)
            self.path_to_knime_workflow = (
                Path(workspace_path) / workflow_path_as_path
            ).resolve()
        else:
            self.path_to_knime_workflow = Path(workflow_path).resolve()
        self.save_after_execution = save_after_execution
        self._data_table_inputs = None
        self._data_table_outputs = None
        self._service_table_input_nodes = None
        self._service_table_output_nodes = None

    def __dir__(self):
        return [ a for a in dir(self.__class__) if a[0] != "_" or a[1] == "_" ]

    def __enter__(self):
        self._discover_inputoutput_nodes()
        return self

    def __exit__(self, exc_type, exc_inst, exc_tb):
        return False

    def _discover_inputoutput_nodes(self):
        self._service_table_input_nodes, self._service_table_output_nodes = \
            find_service_table_node_dirnames(self.path_to_knime_workflow)
        self._input_ids = [
            find_node_id(self.path_to_knime_workflow, stin)
            for stin in self._service_table_input_nodes
        ]
        self._output_ids = [
            find_node_id(self.path_to_knime_workflow, stin)
            for stin in self._service_table_output_nodes
        ]
        self._data_table_inputs = [None] * len(self._service_table_input_nodes)
        self._data_table_outputs = [None] * len(self._service_table_output_nodes)

    def execute(
            self,
            *,
            live_passthru_stdout_stderr=False,
            output_as_pandas_dataframes=True if pandas else False,
        ):
        "Executes the KNIME workflow via KNIME's batch executor."
        outputs = run_workflow_using_multiple_service_tables(
            self.data_table_inputs,
            executable_path,
            self.path_to_knime_workflow,
            self._input_ids,
            self._output_ids,
            save_after_execution=self.save_after_execution,
            live_passthru_stdout_stderr=live_passthru_stdout_stderr,
            output_as_pandas_dataframes=output_as_pandas_dataframes,
        )
        self._data_table_outputs[:] = outputs

    @property
    def data_table_inputs(self):
        """List of inputs (data) to be supplied to the Container Input nodes
        in the KNIME workflow at time of execution.  Growing or shrinking this
        list from its original length is not supported.  This list is not
        guaranteed to persist after __exit__ is called."""
        if self._service_table_input_nodes is None or self._data_table_inputs is None:
            self._discover_inputoutput_nodes()
        return self._data_table_inputs

    @property
    def data_table_outputs(self):
        """List of outputs produced from Container Output nodes in the KNIME
        workflow (populated only after execution).  This list is not
        guaranteed to persist after __exit__ is called."""
        if self._service_table_output_nodes is None or self._data_table_outputs is None:
            self._discover_inputoutput_nodes()
        return self._data_table_outputs

    @property
    def data_table_inputs_names(self):
        "View of which Container Input nodes go with which position in list."
        if self._service_table_input_nodes is None:
            self._discover_inputoutput_nodes()
        return tuple(self._service_table_input_nodes)

    @property
    def data_table_inputs_parameter_names(self):
        if self._service_table_input_nodes is None:
            self._discover_inputoutput_nodes()
        return tuple(
            find_service_table_input_node_parameter_name(
                self.path_to_knime_workflow,
                unique_node_dirname
            )
            for unique_node_dirname in self._service_table_input_nodes
        )

    def _get_workflow_svg(self):
        with open(self.path_to_knime_workflow / "workflow.svg", 'r') as f:
            svg_contents = f.read()
        return svg_contents

    def _adjust_svg(self):
        """As of v3.6.0 the SVGs produced by KNIME all use the same ids for
        clipping paths. This leads to problems when we try and put multiple
        of them on the same page. Here we make those unique across SVGs until
        hopefully KNIME updates its behavior.
        """
        import random
        import string
        chrs = string.ascii_letters + string.digits
        prefix = "".join(random.choice(chrs) for i in range(10))
        svg_contents = self._get_workflow_svg()
        svg_contents = svg_contents.replace('id="clip', 'id="l%sclip' % prefix)
        svg_contents = svg_contents.replace('#clip', '#l%sclip' % prefix)
        return svg_contents

    def _repr_svg_(self):
        "Returns SVG of workflow for subsequent rendering in Jupyter notebook."
        return self._adjust_svg()

    def display_svg(self):
        "Displays SVG of workflow in Jupyter notebook."
        from IPython.display import SVG, display
        display(SVG(self._adjust_svg()))


class RemoteWorkflow(LocalWorkflow):
    "Enables reading and executing of remote KNIME workflows on a Server."

    def __init__(self, workflow_path, *, workspace_path=None,
                 username=None, password=None,
                 server_base_path="/knime"):
        if workspace_path is not None:
            parsed_path = urlparse(workspace_path)
            reduced_workflow_path = workflow_path.split("/knime", 1)[-1]
            self.path_to_knime_workflow = \
                reduced_workflow_path.lstrip("/#").strip("/")
        else:
            parsed_path = urlparse(workflow_path)
            reduced_workflow_path = parsed_path.path.split("/knime", 1)[-1]
            self.path_to_knime_workflow = reduced_workflow_path.strip("/")
            if self.path_to_knime_workflow == "":
                self.path_to_knime_workflow = parsed_path.fragment
        assert parsed_path.scheme.startswith("http"), "Protocol not recognized"
        server_base_path = server_base_path.strip("/")
        self.rest_api_root_url = \
            f"{parsed_path.scheme}://{parsed_path.netloc}/{server_base_path}/rest/v4"
        r = requests.get(
            f"{self.rest_api_root_url}/auth/jwt",
            auth=(username, password)
        )
        assert r.status_code == 200, "Authentication on KNIME Server failed"
        self._last_status_code = r.status_code
        self.jwt = r.text
        self._data_table_inputs = None
        self._data_table_outputs = []
        self._service_table_input_nodes = None

    def _discover_inputoutput_nodes(self):
        r = requests.get(
            f"{self.rest_api_root_url}/repository/{self.path_to_knime_workflow}:openapi",
            headers={"Authorization": f"Bearer {self.jwt}"}
        )
        self._last_status_code = r.status_code
        if r.status_code != 200:
            logging.error(
                "failure querying server for workflow openapi, " \
                f"status_code={r.status_code}, text={r.text!r}"
            )
            raise LookupError(
                f"Server response status code {r.status_code}: {r.text}"
            )

        workflow_openapi = r.json()
        workflow_input_schemas = workflow_openapi['components']['schemas']
        try:
            self._service_table_input_nodes = list(
                workflow_input_schemas['InputParameters']['properties'].keys()
            )
        except KeyError:
            self._service_table_input_nodes = []
        self._data_table_inputs = [None] * len(self._service_table_input_nodes)

    @property
    def data_table_inputs(self):
        """List of inputs (data) to be supplied to the Container Input nodes
        in the KNIME workflow at time of execution.  Growing or shrinking this
        list from its original length is not supported.  This list is not
        guaranteed to persist after __exit__ is called."""
        if (
                self._service_table_input_nodes is None or
                self._data_table_inputs is None
           ):
            self._discover_inputoutput_nodes()
        return self._data_table_inputs

    @property
    def data_table_inputs_parameter_names(self):
        if self._service_table_input_nodes is None:
            self._discover_inputoutput_nodes()
        parameter_names = tuple(
            val.rsplit("-", 1)[0] for val in self._service_table_input_nodes
        )
        return parameter_names

    @property
    def data_table_outputs(self):
        """List of outputs produced from Container Output nodes in the KNIME
        workflow (populated only after execution).  This list is not
        guaranteed to persist after __exit__ is called."""
        return self._data_table_outputs

    def execute(self, *, timeout_ms=-1, reset=None,
                output_as_pandas_dataframes=True if pandas else False):
        "Executes the KNIME workflow via a KNIME Server's REST API."

        data_table_inputs = self.data_table_inputs
        job_input_data = {
            k: convert_dataframe_to_knime_friendly_dict(v)
            for k, v in zip(self._service_table_input_nodes, data_table_inputs)
        }
        for node_id, input_dt in job_input_data.items():
            if input_dt is None:
                # Trigger warning just like for local workflows.
                warnings.warn(f'No input set for node_id={node_id}', UserWarning)
        job_params = { "timeout": int(timeout_ms) }
        if reset:
            job_params["reset"] = bool(reset)

        r = requests.post(
            f"{self.rest_api_root_url}/repository/{self.path_to_knime_workflow}:execution",
            json=job_input_data,
            params=job_params,
            headers={
                "Authorization": f"Bearer {self.jwt}",
                "Content-Type": "application/json",
                "Accept": "application/vnd.mason+json",
            }
        )
        self._last_status_code = r.status_code
        if r.status_code != 200:
            logging.error(
                "failure during remote job execution, " \
                f"status_code={r.status_code}, text={r.text!r}"
            )
            raise RuntimeError(
                f"Server response status code {r.status_code}: {r.text}"
            )

        rest_service_output = r.json()
        knime_outputs = []
        if output_as_pandas_dataframes:
            try:
                for output in rest_service_output["outputValues"].values():
                    df_columns = list(
                        k for d in output['table-spec']
                        for k, v in d.items()
                    )
                    knime_outputs.append(
                        pandas.DataFrame(
                            output['table-data'],
                            columns=df_columns
                        )
                    )
            except ImportError:
                logging.warning("requested output as DataFrame not possible")
            except KeyError:
                # If no Container Output (Table) nodes exist in a workflow,
                # the "outputValues" key will not appear in returned json.
                logging.info("no output data tables produced by workflow")
            except Exception as e:
                logging.error("error while converting KNIME output to DataFrame")
                raise e
        else:
            knime_outputs = [
                out["table-data"]
                for out in rest_service_output["outputValues"].values()
            ]
        self._data_table_outputs[:] = knime_outputs

    def _get_workflow_svg(self):
        r = requests.get(
            f"{self.rest_api_root_url}/repository/{self.path_to_knime_workflow}:image",
            headers={"Authorization": f"Bearer {self.jwt}"}
        )
        self._last_status_code = r.status_code
        if r.status_code != 200:
            logging.error(
                "failure during access of remote workflow SVG, " \
                f"status_code={r.status_code}, text={r.text!r}"
            )
            raise LookupError(
                f"Server response status code {r.status_code}: {r.text}"
            )
        svg_contents = r.text
        return svg_contents
