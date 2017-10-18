Download the latest elastic stack
=================================

*requires python3*

This downloads, extracts, and installs the x-pack plugin. You can install with
pip3, just ``pip3 install .`` from the project’s root directory (the location
of this README). After doing so, you can call from command line with
``$ elastic_stack_download``. If you wanted to run directly without installing
(development) just use ``python3 -m download-runner``.

You can pass in a specific version using ``-v`` or ``--version`` arguments,
otherwise it will hit the elasticsearch download page to try to parse out the
latest stack release version.

The output directory by default is ``~/builds/``. Use ``--output_dir`` to override.

**Note: Kibana x-pack plugin will take a couple minutes, be patient**

------------

todo:

- Install minimal configuration options from external ``.yml`` file for each
  of the stack products.
- Code cleanup and tests.


------------
