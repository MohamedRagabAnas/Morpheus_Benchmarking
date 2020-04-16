# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import csv


with open('person_studyAt_organisation_0_0.csv',encoding="utf8") as studyat_in, open("person_studyat_university.csv", 'w',encoding="utf8",newline='') as f_out:
    read_studyAt_org = csv.reader(studyat_in, delimiter='|')
    writer = csv.writer(f_out,delimiter='|')
    
    next(read_studyAt_org,None)    
    writer.writerow([":START_ID(PERSON_ID)",":END_ID(UNIVERSITY_ID)","classYear:int",":TYPE"])
    
    for row in read_studyAt_org:
            writer.writerow(row)
