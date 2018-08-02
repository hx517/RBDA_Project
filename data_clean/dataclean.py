from __future__ import print_function
from pyspark import SparkContext
from csv import reader

import sys

import re
import datetime

class Point:
    def __init__(self,x,y):
        self.x = x
        self.y = y

#在函数中如果为null “”“ ”，返回false

#检查投诉号 x[0]有效性
def is_valid_cmplnt_num(x):
	if x is "" or x is " ":
		return False
    #使用正则式检查
	elif re.match('[1-9][0-9]+$',x):
		return True
	else :
		return False

#功能检查 日期有效性 for x[1], x[3] and x[5]
def is_valid_date(x):
	if x is "" or x is " ":
		return False
	else :
            y=x
            x=x.split("/")
            try:
                year=int(x[2])
                month=int(x[0])
                day= int(x[1])
                #年份 特定区间 2006 到 2016, 其他无效
                if year >=2006 and year <=2016 :
                    try:
                        newDate= datetime.datetime(year,month,day)
                        return True
                    except :
                        return False
                else :
                    return False
            except:
                return False

#功能检查 时间有效性 x[2] and x[4]
def is_valid_time(x):
    if x is "" or x is " ":
        return False
    else :
            y=x
            x=x.split(":")
            try:
                hour=int(x[0])
                mins=int(x[1])
                secs= int(x[2])
                # if hours is 24 then change it to 0 hours
                if hour == 24 and mins== 0 and secs == 0:
                    hour=0
                try:
                        newTime= datetime.time(hour,mins,secs)
                        return True
                except :
                        return False
            except:
                return False


#功能检查 key code PD_CD x[6] and x[8]
def is_valid_key_code(x):
	if x is "" or x is " ":
		return False
    #check using regular expression
	elif re.match('[0-9][0-9][0-9]$',x):
		return True
	else :
		return False

#检查攻击，PD和管辖权描述 x[7], x[9] and x[12]
#also x[16], x[17] and x[18]
def is_valid_string(x):
    if x is "" or x is " ":
        return False
    else:
        return True

#检查功能是否表明犯罪已完成或已经尝试 x[10]
def to_check_cmplt_attmpt_code(x):
    if x is "" or x is " ":
        return False
    else:
        attmpt_code_list=['COMPLETED',"ATTEMPTED"]
        if x not in attmpt_code_list:
            return False
        else :
            return True

#检查查攻击等级x的功能 x[11]
def to_check_cat_code(x):
    if x is "" or x is " ":
        return False
    else:
        cat_crimes_list=['FELONY',"MISDEMEANOR","VIOLATION"]
        if x not in cat_crimes_list:
            return False
        else :
            return True

#检查 borough names x[13]
def is_valid_borough_name(x):
    if x is "" or x is " ":
        return False
    else:
        borough_names_list=["MANHATTAN",'BRONX',"BROOKLYN","QUEENS","STATEN ISLAND"]
        if x not in borough_names_list:
            return False
        else :
            return True

#检查地址代码有效性 x[14]
def is_valid_precinct_code(x):
    if x is "" or x is " ":
        return False
    #check using regular expression
    elif re.match('^[0-9]+$',x):
        return True
    else :
        return False

#检查位置出现有效性 x[15]
def is_valid_loc_of_occurance(x):
    if x is "" or x is " ":
        return False
    else:
        locations_list=['FRONT OF','OPPOSITE OF','REAR OF','INSIDE','OUTSIDE']
        if x not in locations_list:
            return False
        else :
            return True

#经纬度位置有效性
lat_max=40.917577
lat_min=40.477399

long_max=-73.700009
long_min=-74.25909

#检查NYC边界是否存在lat long值
def is_present_nyc(point):
    if(point.x<lat_min or point.x>lat_max):
      return False
    if(point.y<long_min or point.y> long_max):
      return False
    return True

#检查lat long值有效性 x[23]
def to_check_lat_long(x):
    if x== "" or x== " "or x=="\t":
        return False
    x=x.strip("'")
    x=x.replace("(","")
    x=x.replace(")","")
    lat,lon=x.split(",")
    lat=lat.strip()
    lon=lon.strip()
    try:
        lat=float(lat)
        lon=float(lon)
        if is_present_nyc(Point(lat,lon)) :
            return True
        else:
            return False
    except :
        return False

#检查所有功能
def all_checker(x):
	if is_valid_cmplnt_num(x[0]) and is_valid_date(x[1]) and is_valid_time(x[2]) and is_valid_key_code(x[6]) and is_valid_string(x[7]) and is_valid_key_code(x[8]) and is_valid_string(x[9]) and to_check_cmplt_attmpt_code(x[10]) and to_check_cat_code(x[11]) and is_valid_string(x[12]) and is_valid_borough_name(x[13]) and is_valid_precinct_code(x[14]) and is_valid_loc_of_occurance(x[15]) and is_valid_string(x[16]) and to_check_lat_long(x[23]):
		return True
	else :
		return False

#csv转回
def convert_to_csv_line(data):
    for i in range(len(data)):
        if "," in str(data[i]) :
            data[i]='"'+data[i]+'"'
    return ','.join(d for d in data)

#main function
if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x)).map(lambda x: (x,all_checker(x))).filter(lambda x: x[1]==True).map(lambda x: convert_to_csv_line(x[0]))

    lines.saveAsTextFile('cleaned_data.csv')
