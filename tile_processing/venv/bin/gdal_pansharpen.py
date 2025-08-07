#!/Users/noahgans/Desktop/comunity_view_backend/tile_cycle/venv/bin/python3.11

import sys

from osgeo.gdal import UseExceptions, deprecation_warn

# import osgeo_utils.gdal_pansharpen as a convenience to use as a script
from osgeo_utils.gdal_pansharpen import *  # noqa
from osgeo_utils.gdal_pansharpen import main

UseExceptions()

deprecation_warn("gdal_pansharpen")
sys.exit(main(sys.argv))
