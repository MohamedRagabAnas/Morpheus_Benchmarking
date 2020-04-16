# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import csv


with open('place_0_0.csv',encoding="utf8") as f_in, open("city.csv", 'w',encoding="utf8") as f_out_1, open("country.csv", 'w',encoding="utf8") as f_out_2, open("continent.csv", 'w',encoding="utf8") as f_out_3:
    readCSV = csv.reader(f_in, delimiter='|')
    
    f_out_1.write('id:ID(CITY_ID)|name:string|url:string|:LABEL')
    f_out_2.write('id:ID(COUNTRY_ID)|name:string|url:string|:LABEL')
    f_out_3.write('id:ID(CONTINENT_ID)|name:string|url:string|:LABEL')
    
    cities_list=[]
    countries_list=[]
    continent_list=[]
    

    for rownum, row in enumerate(readCSV):
        newline=[]
        
        if "city" in row:
            newline.append(row[0])
            newline.append(row[1])
            newline.append(row[2])
            newline.append("City")
            cities_list.append(newline)
            
        if "country" in row:
            newline.append(row[0])
            newline.append(row[1])
            newline.append(row[2])
            newline.append("Country")
            countries_list.append(newline)
            
        elif "continent" in row:
            newline.append(row[0])
            newline.append(row[1])
            newline.append(row[2])
            newline.append("Continent")
            continent_list.append(newline)

        
    for continent in continent_list:
        f_out_3.write("\n")
        for i, continet_col in enumerate(continent):
            f_out_3.write(continet_col+ ("|" if i < len(continent)-1 else ""))
     
    for city in cities_list:
        f_out_1.write("\n")
        for i, city_col in enumerate(city):
            f_out_1.write(city_col+ ("|" if i < len(city)-1 else ""))
   
    for country in countries_list:
        f_out_2.write("\n")
        for i, country_col in enumerate(country):
            f_out_2.write(country_col+ ("|" if i < len(country)-1 else ""))    