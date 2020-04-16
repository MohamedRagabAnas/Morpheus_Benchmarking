# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import csv


with open('person_workAt_organisation_0_0.csv',encoding="utf8") as workat_in, open("person_workat_company.csv", 'w',encoding="utf8",newline='') as f_out:
    read_workAt_org = csv.reader(workat_in, delimiter='|')
    writer = csv.writer(f_out,delimiter='|')
    
    next(read_workAt_org,None)    
    writer.writerow([":START_ID(PERSON_ID)",":END_ID(COMPANY_ID)","workFrom:int",":TYPE"])
    
    for row in read_workAt_org:
            writer.writerow(row)
