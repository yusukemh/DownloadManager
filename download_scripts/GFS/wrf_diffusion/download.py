from download_scripts.GFS.gfs_download_manager import GFSForecastDownloadManager
from download_scripts.GFS.config import VARIABLES, REGION_BOUND
from download_manager.utils import ArrayParser
import argparse

def download(year, month):
    dm = GFSForecastDownloadManager(base_dir=f"/mnt/lustre/koa/koastore/sadow_group/shared/wrf_hawaii/raw_data/gfs/", 
                                    buffer_dir='/mnt/lustre/koa/scratch/yusukemh/wrf-diffusion/gfs_buffer',
                                    log_filename='stdout', forecast_horizon_hours=range(49), issue_times=[0,6,12,18],
                                    variables=VARIABLES, region_bound=REGION_BOUND)
    
    dm.download_files(mode='monthly', year=year, month=month)
    dm.cleanup()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', help='SLURM TASK ID', type=int, default=-1)
    task_id = parser.parse_args().id

    array_parser = ArrayParser(year=range(2002, 2026), month=range(1,13))
    kwargs = array_parser[task_id]
    download(**kwargs)

if __name__ == '__main__':
    main()