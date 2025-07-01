from extraction_from_mysql import extract 
from transform import transfrom
from extraction_from_SSMS import extract_mss
from load_to_SSMS import load_to_ssm
from load_to_mysql import load_mysql


data=extract()
data2=extract_mss()
data1,data2=transfrom(data)
load_to_ssm(data1,data2)
load_mysql(data2)
