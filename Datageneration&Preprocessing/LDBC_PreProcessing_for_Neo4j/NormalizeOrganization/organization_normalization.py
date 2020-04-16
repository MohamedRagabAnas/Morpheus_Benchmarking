# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import csv


with open('organisation_0_0.csv',encoding="utf8") as f_in, open("university.csv", 'w',encoding="utf8") as f_out_1, open("company.csv", 'w',encoding="utf8") as f_out_2:
    readCSV = csv.reader(f_in, delimiter='|')
    
    f_out_1.write('id:ID(UNIVERSITY_ID)|name:string|url:string|:LABEL')
    f_out_2.write('id:ID(COMPANY_ID)|name:string|url:string|:LABEL')
    
    univ_list=[]
    comp_list=[]

    

    
    for rownum, row in enumerate(readCSV):
        newline=[]
        
        if "university" in row:
            newline.append(row[0])
            newline.append(row[2])
            newline.append(row[3])
            newline.append("University")
            univ_list.append(newline)

            
        if "company" in row:
            newline.append(row[0])
            newline.append(row[2])
            newline.append(row[3])
            newline.append("Company")
            comp_list.append(newline)

            
        
    for university in univ_list:
        f_out_1.write("\n")
        for i, univ_col in enumerate(university):
            f_out_1.write(univ_col+ ("|" if i < len(university)-1 else ""))
     
    for company in comp_list:
        f_out_2.write("\n")
        for i, company_col in enumerate(company):
            f_out_2.write(company_col+ ("|" if i < len(company)-1 else ""))