from download_manager import DownloadManager, GFSForecastMetadata
# from download_manager.file_metadata import GFSForecastMetadata
import pandas as pd
import xarray as xr
import pygrib
from pathlib import Path
import cfgrib
import os
import re
from datetime import datetime
from nimbus.common.io import run_command