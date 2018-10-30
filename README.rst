.. _KNIME: https://www.knime.com/
.. _Python: https://www.python.org/
.. _pandas: https://pandas.pydata.org/

====================================
knime(py):  Python toolkit for KNIME
====================================

knime(py) provides tools for reading and executing KNIME_ workflows from Python_.  It is distributable as a single file module and has no requirements beyond Python_ 3.6+ and the `Python Standard Library <http://docs.python.org/library/>`_.  Optionally, if the pandas_ module is also installed, then pandas DataFrames are supported for both input and output to KNIME workflows executed through this toolkit.


Example: Execute a KNIME Workflow
---------------------------------

.. code-block:: python

  import knime

  with knime.Workflow("DemoWorkflow01") as wf:
      wf.execute()
      results = wf.data_table_outputs[:]


Download and Install
--------------------

.. __: https://github.com/applio/knimepy/raw/master/knime.py

Install the latest stable release with ``pip install knime`` (or ``pip3 install knime`` if you have both Python 2 and 3 installed).  Alternatively, download `knime.py`__ (unstable) into your project directory.  There are no hard dependencies other than Python 3.6+ and the Python standard library itself.


License
-------

.. __: https://github.com/applio/knimepy/raw/master/LICENSE

Code and documentation are available according to the LICENSE__.


Expanded Example: Multiple Inputs/Outputs when Executing a KNIME Workflow
-------------------------------------------------------------------------

.. code-block:: python

  import knime
  import pandas as pd

  # Change the executable_path to point at a particular KNIME install.
  # May alternatively be set via OS Environment Variable, 'KNIME_EXEC'.
  knime.executable_path = r"C:\Program Files\KNIME\knime.exe"

  # Prepare input data tables as DataFrames or regular dicts (in KNIME's
  # required schema) to be read by the "Container Input (Table)" nodes in
  # the KNIME Workflow.
  input_table_1 = pd.DataFrame([["blau", -273.15], ["gelb", 100.0]], columns=["color", "temp"])
  input_table_2 = {
      "table-spec": [{"color": "string"}, {"size": "long"}],
      "table-data": [["blue", 42], ["yellow", 8675309]]
  }

  # Use a with-statement to set the inputs, execute, and get the results.
  with knime.Workflow(r"C:\Users\berthold\knime-workspace\ExploreData01") as wf:
      wf.data_table_inputs[0] = input_table_1
      wf.data_table_inputs[1] = input_table_2
      wf.execute()
      output_table = wf.data_table_outputs[0]  # output_table will be a pd.DataFrame

