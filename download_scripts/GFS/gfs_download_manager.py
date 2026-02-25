from download_manager import DownloadManager, GFSForecastMetadata
import pandas as pd
from pathlib import Path
import os, re
from datetime import datetime
from nimbus.common.io import run_command

class GFSForecastDownloadManager(DownloadManager):
    def __init__(
            self, 
            base_dir,
            buffer_dir,
            forecast_horizon_hours,
            issue_times,
            variables,
            region_bound,
            log_filename,
        ):
        super().__init__(base_dir=base_dir, buffer_dir=buffer_dir, db_field_dtypes=GFSForecastMetadata.dtypes, log_filename=log_filename)
        self.variables = variables
        self.region_bound = region_bound
        self.forecast_horizon_hours = forecast_horizon_hours
        self.issue_times = issue_times

    def callback(self, source_filename, buffer_filename, local_filename):
        def parse_source_filename(url):
            # E.g. "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20220626/00/atmos/gfs.t00z.pgrb2.0p25.f024"
            
            d = re.search(r'\.(\d{8})/', url).group(1)    # look for . followed by 8 digits
            h = re.search(r'/(\d{2})/', url).group(1)     # look for two digits surrounded by two /'s
            f = int(re.search(r'\.f(\d{3})', url).group(1)) # look for . followed by 3 digits
            t0 = datetime.strptime(d + h, "%Y%m%d%H")
            return t0, f

        temp_filename = f"{buffer_filename}.partial.grib2"
        # filter_key = f":({'|'.join(PRESSURE_VARIABLE_KEYS)}):({'|'.join(map(str, PLEVELS))}) mb:|" + '|'.join(SURFACE_VARIABLES)
        filter_key = '|'.join(self.variables)
        Path(local_filename).parent.mkdir(parents=True, exist_ok=True)
        compress_command = f"wgrib2 {buffer_filename} -match '{filter_key}' -small_grib {self.region_bound} {temp_filename}"
        conversion_command = f"wgrib2 {temp_filename} -netcdf {local_filename}"

        run_command(compress_command)
        run_command(conversion_command)

        Path(buffer_filename).unlink()
        Path(temp_filename).unlink()

        utc_issue_timestamp, forecast_horizon = parse_source_filename(source_filename)
        return GFSForecastMetadata(
                source_filename=source_filename,
                local_filename=local_filename,
                size=os.stat(local_filename).st_size,
                last_modified=pd.Timestamp.now('UTC'),
                utc_issue_timestamp=utc_issue_timestamp,
                forecast_horizon=forecast_horizon
            )

    def _calculate_filenames_for_range(self, start: pd.Timestamp, end: pd.Timestamp):
        utc_issue_timestamps = pd.date_range(start.floor('D'), end.ceil('D'), freq='6h')
        utc_issue_timestamps = utc_issue_timestamps[
            utc_issue_timestamps.hour.isin(self.issue_times)
            & (utc_issue_timestamps >= start)
            & (utc_issue_timestamps <= end)]

        source_filenames, buffer_filenames, local_filenames, db_filenames = [], [], [], []
        for utc_issue_timestamp in utc_issue_timestamps:
            for forecast_horizon_hour in self.forecast_horizon_hours:
                str_format = utc_issue_timestamp.strftime(f"gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25.f{forecast_horizon_hour:03d}")
                source_filenames.append(f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/{str_format}")
                buffer_filenames.append(str_format)
                local_filenames.append(utc_issue_timestamp.strftime(f"%Y_%m/%d/%Y_%m_%d_%H:00_f{forecast_horizon_hour:03d}.nc"))
                db_filenames.append(utc_issue_timestamp.strftime("%Y_%m.db"))

        return source_filenames, buffer_filenames, local_filenames, db_filenames

    def cleanup(self):
        root = Path(self.buffer_dir)
        for path in sorted(root.rglob('*'), key=lambda p: len(p.parts), reverse=True): # deepest directories first
            if path.is_dir() and not any(path.iterdir()): # remove if empty (Path.rmdir() cannot remove non-empty dir anyways, so it is safe.)
                path.rmdir()