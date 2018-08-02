#count for each type in few important columns for every month of every year
#<column> <year> <month> <type> <count>
#Considered in this program are Column no 3, 5,  15 and 16
from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

# CMPLNT_NUM	Randomly generated persistent ID for each complaint 
# CMPLNT_FR_DT	    Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
# CMPLNT_FR_TM	    Exact time of occurrence for the reported event (or starting time of occurrence, if CMPLNT_TO_TM exists)
# CMPLNT_TO_DT	    Ending date of occurrence for the reported event, if exact time of occurrence is unknown
# CMPLNT_TO_TM    	Ending time of occurrence for the reported event, if exact time of occurrence is unknown
# RPT_DT	            Date event was reported to police 
# KY_CD	            Three digit offense classification code
# OFNS_DESC	        Description of offense corresponding with key code
# PD_CD	            Three digit internal classification code (more granular than Key Code)
# PD_DESC	            Description of internal classification corresponding with PD code (more granular than Offense Description)
# CRM_ATPT_CPTD_CD	Indicator of whether crime was successfully completed or attempted, but failed or was interrupted prematurely
# LAW_CAT_CD	        Level of offense: felony, misdemeanor, violation 
# JURIS_DESC	        Jurisdiction responsible for incident. Either internal, like Police, Transit, and Housing; or external, like Correction, Port Authority, etc.
# BORO_NM	            The name of the borough in which the incident occurred
# ADDR_PCT_CD	        The precinct in which the incident occurred
# LOC_OF_OCCUR_DESC	Specific location of occurrence in or around the premises; inside, opposite of, front of, rear of
# PREM_TYP_DESC	    Specific description of premises; grocery store, residence, street, etc.
# PARKS_NM	        Name of NYC park, playground or greenspace of occurrence, if applicable (state parks are not included)
# HADEVELOPT	        Name of NYCHA housing development of occurrence, if applicable
# X_COORD_CD	        X-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)
# Y_COORD_CD	        Y-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)
# Latitude	        Latitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326) 
# Longitude	        Longitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)


columns = {
        0: 'CMPLNT_NUM',
        1: 'CMPLNT_FR_DT',
        2: 'CMPLNT_FR_TM',
        3: 'CMPLNT_TO_DT',
        4: 'CMPLNT_TO_TM',
        5: 'RPT_DT',
        6: 'KY_CD',
        7: 'OFNS_DESC',
        8: 'PD_CD',
        9: 'PD_DESC',
        10: 'CRM_ATPT_CPTD_CD',
        11: 'LAW_CAT_CD',
        12: 'JURIS_DESC',
        13: 'BORO_NM',
        14: 'ADDR_PCT_CD',
        15: 'LOC_OF_OCCUR_DESC',
        16: 'PREM_TYP_DESC',
        17: 'PARKS_NM',
        18: 'HADEVELOPT',
        19: 'X_COORD_CD',
        20: 'Y_COORD_CD',
        21: 'Latitude',
        22: 'Longitude',
        23: 'Lat_Lon'
}

def parse_col(col):
    list = []
    dateinfo = col[1].split('/')
    date = dateinfo[2]+' '+dateinfo[0]
    for i in [3, 5, 15, 16]:
        list.append(columns[i]+' '+date+' '+(col[i] if col[i] != '' else 'undefined'))

    return list

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_type_col_monthly.py <clean_dataset>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).flatMap(parse_col)
    counts = lines.map(lambda column : (column, 1)).reduceByKey(add).sortBy(lambda x: x[0], False)
    counts = counts.map(lambda x: x[0]+'\t'+str(x[1]))
    counts.saveAsTextFile("imp_col_type_monthly_count.out")
    sc.stop()
