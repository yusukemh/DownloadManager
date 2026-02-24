VARIABLES = [ # I know, this looks repetitive but trust me this is the most flexible way...
    # Check https://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs.t00z.pgrb2.0p25.f003.shtml at https://www.nco.ncep.noaa.gov/pmb/products/gfs/ for variable names.
    # Pressure level variables
    ":HGT:200 mb:",  ":HGT:300 mb:",  ":HGT:500 mb:",  ":HGT:700 mb:",  ":HGT:850 mb:",  ":HGT:950 mb:",  ":HGT:1000 mb:",  # HGT: geopotential height [gpm]
    ":SPFH:200 mb:", ":SPFH:300 mb:", ":SPFH:500 mb:", ":SPFH:700 mb:", ":SPFH:850 mb:", ":SPFH:950 mb:", ":SPFH:1000 mb:", # SPFH: specific humidity [kg/kg]
    ":VVEL:200 mb:", ":VVEL:300 mb:", ":VVEL:500 mb:", ":VVEL:700 mb:", ":VVEL:850 mb:", ":VVEL:950 mb:", ":VVEL:1000 mb:", # VVEL: Vertical Velocity [Pa/s]
    ":TMP:200 mb:",  ":TMP:300 mb:",  ":TMP:500 mb:",  ":TMP:700 mb:",  ":TMP:850 mb:",  ":TMP:950 mb:",  ":TMP:1000 mb:",  # TMP: Temperature [K]
    ":UGRD:200 mb:", ":UGRD:300 mb:", ":UGRD:500 mb:", ":UGRD:700 mb:", ":UGRD:850 mb:", ":UGRD:950 mb:", ":UGRD:1000 mb:", # UGRD: U-component of wind [m/s]
    ":VGRD:200 mb:", ":VGRD:300 mb:", ":VGRD:500 mb:", ":VGRD:700 mb:", ":VGRD:850 mb:", ":VGRD:950 mb:", ":VGRD:1000 mb:", # VGRD: V-component of wind [m/s]
    # Surface variables
    ":UGRD:10 m above ground:", # U-wind at 10 m
    ":VGRD:10 m above ground:", # V-wind at 10 m
    ":TMP:2 m above ground:",   # air temp. at 2m
    ":PWAT:"                    # Precipitable water [kg / m^2]
]
REGION_BOUND = "198.5:207.5 17.5:23.5"