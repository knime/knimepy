===================
Tests for knime(py)
===================


Environment Variables
---------------------

Local workflow execution tests may need 1 environment variable set.  When running the remote workflow execution tests, at least 3 environment variables must be set:

* ``KNIME_EXEC`` optionally sets the location of the KNIME executable, used when running local workflow execution tests
* ``KNIME_SERVER_URLROOT`` sets the root url of the KNIME Server, used when running remote workflow execution tests
* ``KNIME_SERVER_USER`` sets the user name to be used in accessing the KNIME Server when running the remote tests
* ``KNIME_SERVER_PASS`` sets the user's password to be used in accessing the KNIME Server when running the remote tests
* ``KNIME_SERVER_TESTDIR`` optionally sets the location where testing workflows have been placed on the KNIME Server (defaults to ``/Users/{KNIME_SERVER_USER}``)

Example settings for these environment variables::

    KNIME_EXEC=/Users/guest/knime/KNIME 3.7.0.app/Contents/MacOS/Knime
    KNIME_SERVER_URLROOT=https://mytestingserver.knime.org/knime
    KNIME_SERVER_USER=guest
    KNIME_SERVER_PASS=swordfish
    KNIME_SERVER_TESTDIR=/Users/guest


Running Tests
-------------

Local workflow execution tests::

    % python tests/test_core.py -v

Remote workflow execution tests::

    % python tests/test_remote_workflow.py -v

All combined tests::

    % python setup.py test
