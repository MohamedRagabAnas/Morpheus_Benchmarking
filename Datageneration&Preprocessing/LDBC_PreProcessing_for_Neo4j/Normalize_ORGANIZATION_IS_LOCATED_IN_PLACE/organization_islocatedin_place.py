# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd

data_OrgInPlace = pd.read_csv("organisation_isLocatedIn_place_0_0.csv", delimiter='|')
data_city= pd.read_csv("city.csv", delimiter='|')
data_country= pd.read_csv("country.csv", delimiter='|')



data_city.set_index('id:ID(CITY_ID)', inplace = True)
data_country.set_index('id:ID(COUNTRY_ID)', inplace = True)


uni_city = data_OrgInPlace.join(data_city, on = ':END_ID(PLACE_ID)', how='inner')[[':START_ID(ORGANISATION_ID)', ':END_ID(PLACE_ID)', ':TYPE']]
uni_city.columns = [':START_ID(UNIVERSITY_ID)', ':END_ID(CITY_ID)', ':TYPE']
uni_city.to_csv('university_in_city.csv', sep  = '|', index=False)


comp_country = data_OrgInPlace.join(data_country, on = ':END_ID(PLACE_ID)', how='inner')[[':START_ID(ORGANISATION_ID)', ':END_ID(PLACE_ID)', ':TYPE']]
comp_country.columns = [':START_ID(COMPANY_ID)', ':END_ID(COUNTRY_ID)', ':TYPE']
comp_country.to_csv('company_in_country.csv', sep  = '|', index=False)