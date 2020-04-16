# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import csv


with open('post_isLocatedIn_place_0_0.csv',encoding="utf8") as islocated_in, open("post_islocatedin_country.csv", 'w',encoding="utf8",newline='') as f_out:
    read_islocatedin_place = csv.reader(islocated_in, delimiter='|')
    writer = csv.writer(f_out,delimiter='|')
    
    next(read_islocatedin_place,None)    
    writer.writerow([":START_ID(POST_ID)",":END_ID(COUNTRY_ID)",":TYPE"])
    
    for row in read_islocatedin_place:
            writer.writerow(row)
