from dataclasses import dataclass, field, fields
import pandas as pd
from datetime import datetime

@dataclass
class FileMetadata():
    """Python class to handle file metadata and their dtypes, which informs data types for the SQL table. 
    """
    dtypes = dict(# dtype for sql database.
        product='TEXT',             # ['GOES', 'HRRR', 'GFS', ...]
        datatype='TEXT',            # ['observation', 'forecast', 'reanalysis']
        source_filename='TEXT',     # the source filename from which the file was trieved.
        local_filename='TEXT',      # the local filename where the file is stored
        size='INT',                 # file bytesize
        last_modified='DATETIME'    # the datetime at which the file was last modified
    )

    product: str
    datatype: str
    source_filename: str
    local_filename: str
    size: int
    last_modified: pd.Timestamp

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}
    
    def __post_init__(self):
        for field, field_type in self.__annotations__.items():
            value = getattr(self, field)
            
            # Type check
            if not isinstance(value, field_type):
                raise TypeError(f"{field} must be {field_type.__name__}, got {type(value).__name__}")

        if not self.product in ['GFS', 'HRRR', 'GOES']:
            raise ValueError(f"Expected attribute 'product' in ['GFS', 'HRRR', 'GOES']. Got {self.product=} instead.")
        if not self.datatype in ['observation', 'forecast', 'reanalysis']:
            raise ValueError(f"Expected attribute 'datatype' in ['observation', 'forecast', 'reanalysis'], gor {self.datatype} instead.")


@dataclass
class GFSForecastMetadata(FileMetadata):
    # additional Metadata attributes
    forecast_horizon: int
    utc_issue_timestamp: datetime
    # overwrite default values
    product: str = field(default='GFS', init=False)
    datatype: str = field(default='forecast', init=False)
    # register dtypes for the additional Metadata attributes
    dtypes = FileMetadata.dtypes | dict(forecast_horizon='INT', utc_issue_timestamp='DATETIME')