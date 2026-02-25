from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlretrieve, urlopen
from pathlib import Path
import os, itertools, time, urllib, logging, base64
from http.cookiejar import CookieJar
from abc import abstractmethod
from .file_metadata import FileMetadata
import pandas as pd

# NimbusAI sdk packages
from nimbus.common.io import init_logging
from nimbus.common.tools import param_constraints
from nimbus.common.exception import MaximumTrialExceededError, IncompleteDownloadError, get_exception_text
# from nimbus.common.file_metadata import FileMetadata
import nimbus.common.sql as sql

class DownloadManager():
    """Parent class ob an onject, designed to take care of a large number of large files.
    Some of the features includes:
        1. Parallelization.
        2. Duplicate file detection.
        3. File cropping to save storage. --deprecated--
        4. Corrupt file detection.
        5. Error handling.
        6. Creation and maintenance of database (SQL table).
        7. Process logs.
    Supports live-download and historical download.
    """
    def __init__(self,
                 base_dir: str,
                 log_filename: str,
                 db_field_dtypes: dict,#sql compatible
                 buffer_dir: str=None
        ):
        if not os.path.exists(base_dir):
            # prevent accidentally creating whole new storage by typo
            raise OSError(f"{base_dir} does not exist.")

        self.base_dir = base_dir

        if log_filename != 'stdout':
            if not Path(log_filename).is_absolute():
                raise ValueError(f"log_filename must be absolute path.")
            if not Path(log_filename).suffix == '.log':
                raise ValueError(f"log_filename must have '.log' extension.")
        init_logging(log_filename, level=5, tz="HST")
        
        self.db_field_dtypes = db_field_dtypes
        self.database_dir = f"{base_dir}/database/"
        self.data_dir  = f"{base_dir}/data/"
        self.buffer_dir = buffer_dir


    """####################################################"""
    """ --- Functions to be implemented by subclasses. --- """
    """####################################################"""


    @staticmethod
    def callback(source_filename, buffer_filename, local_filename) -> FileMetadata:
        # This function is called after downloading individual file.
        # Main purpose is to get the file metadata.
        # Child class can overwrite this function to, e.g.,
        #   1. create coordinate file.
        #   2. raise issue when corrupt file is detected.
        #   3. delete buffer files
        raise NotImplementedError()


    @abstractmethod
    def _calculate_filenames_for_range(self, start: pd.Timestamp, end: pd.Timestamp):
        """Given start and end timestamps, calculate the filenames to download.
        """
        # return source_filenames, buffer_filenames, local_filenames, db_filenames.
        # Could either be relative or absolute. In case of relative path, it would be relative to self.database_dir or self.data_dir
        raise NotImplementedError("Function _calculate_filenames_for_range needs to be implemented by subclass of DownloadManager.")

    """####################################################"""
    """ ---        Functions that never changes        --- """
    """####################################################"""

    @param_constraints(mode=['latest', 'monthly', 'range'])
    def download_files(self, *, mode, year=None, month=None, backfill_hours=None, start=None, end=None, credentials=None, sequential=False, **kwargs):
        """Downloads files from online source.
        Args:
            mode: One of ['latest', 'monthly', 'range'].
                - 'latest' to download the latest files available since backfill_hours hours ago. 'backfill_hours' cannot be None.
                - 'monthly' to download the entire month data. 'year' and 'month' must be specified.
                - 'range' to download the data from 'start' to 'end'.
            credentials: String in the format of "user:pass" in case authentication is required.
            sequential: whether or not to run in sequence or in parallel. Default=False
        """
        logging.info("Calculating filenames")
        if mode == 'latest':# this if statement might look ugly but necessary for error message readability.
            if backfill_hours is None: raise ValueError("If mode='latest', arg 'backfill_hours' must be specified.")
            source_filenames, buffer_filenames, local_filenames, database_filenames = self.calculate_latest_filenames(backfill_hours=backfill_hours, **kwargs)
        elif mode == 'monthly':
            if year is None: raise ValueError("If mode='monthly', arg 'year' must be specified")
            if month is None: raise ValueError("If mode='monthly', arg 'month' must be specified")
            source_filenames, buffer_filenames, local_filenames, database_filenames = self.calculate_monthly_filenames(year=year, month=month, **kwargs)
        elif mode == 'range':
            if start is None: raise ValueError("If mode='range', arg 'start' must be specified")
            if end is None: raise ValueError("If mode='range', arg 'end' must be specified")
            source_filenames, buffer_filenames, local_filenames, database_filenames = self.calculate_filenames_for_range(start=start, end=end, **kwargs)

        # initialize database. mode='latest' or 'range' could result in multiple databases to be initialized, as database is monthly
        logging.info("Checking database for already existing files.")
        for db_filename in set(database_filenames):
            sql.init_database(db_filename=db_filename, table='file', column_dtype=self.db_field_dtypes, primary_key='source_filename')
        
        # filter down to only files we do not have in database (dne = Does Not Exist)
        dne = [not sql.exists(db_filename=d_filename, table='file', attr='source_filename', val=s_filename)\
                for (d_filename, s_filename) in zip(database_filenames, source_filenames)]
        source_filenames = list(itertools.compress(source_filenames, dne))
        buffer_filenames = list(itertools.compress(buffer_filenames, dne))
        local_filenames = list(itertools.compress(local_filenames, dne))
        database_filenames = list(itertools.compress(database_filenames, dne))

        logging.info(f"Found {len(source_filenames)} new files.")
        if sum(dne) == 0:
            logging.info(f"Completed downloading.")
            return

        if not sequential:
            # use ThreadPoolExecutor to delay rising Errors until all workers have completed their jobs.
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = [exe.submit(
                        download_process_insert,
                        dict(
                            source_filename=source_filename, buffer_filename=buffer_filename, local_filename=local_filename, database_filename=database_filename,
                            credentials=credentials, callback=self.callback
                        )
                    )
                        for (source_filename, buffer_filename, local_filename, database_filename) in \
                            zip(source_filenames, buffer_filenames, local_filenames, database_filenames)
                ]
            # make sure no error happened during download
            for future, source_filename in zip(futures, source_filenames):
                try:
                    result = future.result()# this raises Error in case of Error in a worker
                except Exception as e:
                    # Do not raise Error because that would be inconvenient for live download.
                    logging.error(f"Error occurred while processing {source_filename}.\n{get_exception_text(e)}")
        else:
            raise NotImplementedError("this part probably needs to be checked.")
            for source_filename, local_filename, database_filename in zip(source_filenames, local_filenames, database_filenames):
                download_process_insert(
                    source_filename=source_filename, local_filename=local_filename,
                    database_filename=database_filename, proc_fn=self.process_and_validate_individual_file, credentials=credentials
                )
                logging.info(f"Successfully saved a file locally: {local_filename}")

        logging.info(f"Completed downloading.")
    
    @param_constraints(start=pd.Timestamp, end=pd.Timestamp)
    def calculate_filenames_for_range(self, *, start, end, **kwargs):
        def convert_paths(paths, varname, relative_to):
            ret = []
            for p in paths:
                if p is None:
                    ret.append(None)
                elif Path(p).is_absolute():
                    if not Path(p).relative_to(relative_to):
                        raise ValueError(f"Expected all elements in '{varname}' under {relative_to}, got {p} instead.")
                    ret.append(p)
                else:
                    ret.append(f"{relative_to}/{p}")
            return ret
                
        if start >= end: raise ValueError(f"'start' cannot be after 'end'.")
        # make sure all files are under certain subdir.
        source_filenames, buffer_filenames, local_filenames, db_filenames = self._calculate_filenames_for_range(start, end, **kwargs)
        # convert filenames to absolute paths as necessary.
        buffer_filenames = convert_paths(buffer_filenames, varname='buffer_filenames', relative_to=self.buffer_dir)
        local_filenames = convert_paths(local_filenames, varname='local_filenames', relative_to=self.data_dir)
        db_filenames = convert_paths(db_filenames, varname='db_filenames', relative_to=self.database_dir)

        if self.buffer_dir is None:
            if not all([f is None for f in buffer_filenames]):
                raise ValueError(f"Expected [None, None, ...] for 'buffer_filenames' with DownloadManager.buffer_filename=None. Got {buffer_filenames=} instead.")
    
        return source_filenames, buffer_filenames, local_filenames, db_filenames

    def calculate_monthly_filenames(self, year, month, **kwargs):
        start = pd.Timestamp(year=year, month=month, day=1, hour=0, tz='UTC')
        # MonthEnd(0) prevents overshooting in case utcnow is the last day of a month
        end = start + pd.tseries.offsets.MonthEnd(0) + pd.Timedelta(hours=23)
        utcnow = pd.Timestamp.utcnow().floor('h')
        if start > utcnow: raise ValueError(f"Cannot calculate filenames for future month. Got {year=}, {month=}")
        return self.calculate_filenames_for_range(start=start, end=min(utcnow, end), **kwargs)
    
    # @param_constraints(backfill_hours=int)
    def calculate_latest_filenames(self, backfill_hours, **kwargs):
        """Returns list of source_filenames, local_filenames, database_filenames
        """
        utcnow = pd.Timestamp.utcnow()
        return self.calculate_filenames_for_range(
            start=utcnow - pd.Timedelta(hours=backfill_hours),
            end=utcnow, **kwargs
        )
    
    def export_databases(self):
        db_filenames = self.list_databases()
        if len(db_filenames) == 0:
            return None
        return [sql.export_database(db_filename, table='file') for db_filename in db_filenames]

    def list_databases(self):
        if not os.path.exists(self.database_dir):
            raise FileNotFoundError(f"Expected directory at {self.database_dir}, found nothing. Perhaps this object has never downloaded anything?")
        return [str(Path(f"{self.database_dir}{f}")) for f in os.listdir(self.database_dir)]

# def download_process_insert(source_filename, local_filename, database_filename, callback, credentials):
def download_process_insert(args): # args = dict(source_filename, local_filename, database_filename, callback, credentials)
    source_filename   = args['source_filename']
    buffer_filename   = args['buffer_filename']
    local_filename    = args['local_filename']
    database_filename = args['database_filename']
    callback          = args['callback']
    credentials       = args['credentials']

    Path(buffer_filename).parent.mkdir(exist_ok=True, parents=True)

    # download file
    flag = safe_download_file(source_filename, buffer_filename, credentials=credentials, max_trial=5, http_404_ok=True, exist_ok=False)
    if flag:
        # callback and safe_insert should only happen when files are downloaded successfully.
        file_metadata = callback(source_filename, buffer_filename, local_filename)
        sql.safe_insert(db_filename=database_filename, table='file', data=file_metadata.to_dict())


def safe_download_file(source_filename, dest_filename, credentials=None, max_trial=5, http_404_ok=True, exist_ok=False):
    """Calls download_file() with tolerance (max_trial - many times) in case error occurs.
    To handle incomplete download after process terminated unexpectedly, files are named '---.partial_'. Once download completes it is renamed.
    Returns:
        True: if file is downloaded successfully.
        False: if file is not downloaded but no exception is raised either. This happens if:
            1. The destination file already exists and exist_ok=True (not recommended)
            2. source_filename does not exist.

    Raises exception in two possible scenarios:
        1) FileNotFoundError() in case the source_filename returns 404 error.
        2) MaximumTrialExceededError() in case any other exceptions have been risen for more than max_trial times.
    """
    if os.path.exists(dest_filename):
        if not exist_ok:
            raise FileExistsError(f'File already exists: {dest_filename}')
        logging.warning('exist_ok=True is not recommended. File already exists.')
        return False
    for trial in range(max_trial):
        try:
            # To handle abrupt download interruption, temporarily save file as '.partial_' and rename once download completes.
            download_file(source_filename=source_filename, dest_filename=f"{dest_filename}.partial_", credentials=credentials)
            os.replace(f"{dest_filename}.partial_", dest_filename)
            return True# exit if successful.
        except Exception as e:
            # Check for HTTP 404 status
            if (str(e) == 'HTTP Error 404 Not Found') or (str(e) == 'HTTP Error 404: Not Found'):
                if http_404_ok:
                    logging.warning(f"{source_filename} does not exist.")
                    return False# Simply exit the function. This is expected for live HRRR and GFS download.
                else:
                    raise FileNotFoundError(f"{source_filename} does not exist.") #If 404, no point in trying again. Simply raise Exception.
            
            # Check for maximum trial
            if trial < max_trial:
                logging.warning(f"The following Exception occured at trial {trial + 1}/{max_trial}\n\twhile processing {source_filename}. Retry.")
                logging.warning(f"{get_exception_text(e)}.")
                # sleep for 5 seconds.
                time.sleep(5)
                continue
            else:
                logging.critical(f"Maximum trial reached. The following Exception occured at {trial +1}/{max_trial}\n\twhile processing {source_filename}. Terminating.")
                logging.critical(f"{get_exception_text(e)}.")
                raise MaximumTrialExceededError(f"Extraction failed at the last attemt. Terminating.")


def download_file(source_filename, dest_filename, credentials=None):
    """Makes an attempt to download a file at souce_filename and save locally as dest_filename.
    Pass credentials="username:pass" in case authentication is needed.
    Compares the bytesize on remote and local, and raises IncompleteDownloadError() if mismatch is detented.
    Returns True if successful (for backward compatibility.)

    Here are some helpful links.
    How to add credential header to request:
        https://stackoverflow.com/questions/29708708/http-basic-authentication-not-working-in-python-3-4
    How to avoid 302 error due to redirect not set as cookies on the client side:
        https://stackoverflow.com/questions/32569934/urlopen-returning-redirect-error-for-valid-links
    Turned out this could have been helpful (but not used) to deal with authentication:
        https://urs.earthdata.nasa.gov/documentation/for_users/data_access/python
    """
    if credentials is not None:
        # in case credentials are given, a little work is needed...
        # set up HTTP request with credential header and get bitesize.
        encoded_credentials = base64.b64encode(credentials.encode('ascii'))
        req = urllib.request.Request(source_filename)
        req.add_header(
            "Authorization", f'Basic {encoded_credentials.decode("ascii")}'
        )
        cj = CookieJar() # Cookie setting needed in case of HTTP302 redirect error.
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        # Get the bytesize of the file to be downloaded.
        with opener.open(req) as r:
            bytesize = int(r.info()['Content-Length'])

        # if credentials are given, use request package to download.
        with opener.open(req) as response, open(dest_filename, 'wb') as f_save:
            data = response.read()
            f_save.write(data)
    else:
        # Get the bytesize of the file to be downloaded.
        bytesize = int(urlopen(source_filename).info()['Content-Length'])
        # download.
        urlretrieve(source_filename, dest_filename)
    
    if os.stat(dest_filename).st_size != bytesize:
        raise IncompleteDownloadError(f"Incomplete download for {source_filename}.")

    return True # for backward compatibility