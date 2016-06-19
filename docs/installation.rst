Installation
============

aiorethink is available on `PyPI <https://pypi.python.org/pypi>`_, so you can
install it simply with pip. You can also grab the source right from github.


Prerequisites
-------------

You need at least Python 3.5. Check the version you have like so::

    python3 --version

Along with Python 3.5, you should also have pip.


Obviously, you also need access to an instance of `RethinkDB
<https://www.rethinkdb.com>`_.


Alternative 1: install from PyPI
--------------------------------

::

    pip3 install aiorethink


Alternative 2: install from source
----------------------------------

Running or installing right from `source
<https://github.com/lars-tiede/aiorethink>`_ gives you the possibility to run
the bleeding edge version of aiorethink. There is also more content for you to
play with. For instance, the sources include a `Vagrant
<https://www.vagrantup.com/>`_\file, which gives you a VM running RethinkDB and
all you need for playing with aiorethink.

The easiest way to install aiorethink from source is to use pip to install
aiorethink right from github. To do this in a fresh `virtual environment
<https://docs.python.org/3/library/venv.html>`_, run this in an empty
directory::

    python3 -m venv py-env
    . ./py-env/bin/activate
    pip3 install -e git+https://github.com/lars-tiede/aiorethink@master#egg=aiorethink

You now have aiorethink installed locally. Since we specified ``-e`` ("editable
mode") to pip, the cloned repository will be stored in
``py-env/src/aiorethink``. You can hack things in there if you want. If you
don't need the repository clone, just omit ``-e``.

You can use any version of aiorethink this way, by specifying any git branch,
tag, or commit instead of ``master``.


Alternative 3: hack and build from source
-----------------------------------------

If you don't want the cloned and hackable repository to live in
``py-env/src/aiorethink``, you can also clone the repository yourself and make
an environment that's more tailored towards hacking aiorethink specifically.

Clone the repository::

    git clone https://github.com/lars-tiede/aiorethink

Then cd into the aiorethink directory. Make a virtualenv and install everything
you need into it like so::

    python3 -m venv py-env
    . ./py-env/bin/activate
    pip3 install -r requirements.txt

If you want to run the test suite and build an aiorethink package, install the
pip packages from the other requirements files as well::

    pip3 install -r requirements-test.txt -r requirements-dev.txt

Now you can run a python interpreter and ``import aiorethink`` and start using
aiorethink.

If you want to build an aiorethink distribution package (a "wheel") that you
can install with pip somewhere else (say, your own project's virtualenv), do
this::

    python setup.py bdist_wheel

You'll have a wheel file in the ``build`` directory now. You can install the
wheel somewhere else by pointing pip right to the file::

    pip install PATH_TO_WHEEL_FILE
