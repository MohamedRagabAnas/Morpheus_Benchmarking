# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd


data_Place = pd.read_csv("place_isPartOf_place_0_0.csv", delimiter='|')
data_city= pd.read_csv("city.csv", delimiter='|')
data_country= pd.read_csv("country.csv", delimiter='|')



data_city.set_index('id:ID(CITY_ID)', inplace = True)
data_country.set_index('id:ID(COUNTRY_ID)', inplace = True)


city_country = data_Place.join(data_city, on = ':START_ID(PLACE_ID)', how='inner')[[':START_ID(PLACE_ID)', ':END_ID(PLACE_ID)', ':TYPE']]


city_country.columns = [':START_ID(CITY_ID)', ':END_ID(COUNTRY_ID)', ':TYPE']
city_country.to_csv('city_in_country.csv', sep  = '|', index=False)


country_continent = data_Place.join(data_country, on = ':START_ID(PLACE_ID)', how='inner')[[':START_ID(PLACE_ID)', ':END_ID(PLACE_ID)', ':TYPE']]
country_continent.columns = [':START_ID(COUNTRY_ID)', ':END_ID(CONTINENT_ID)', ':TYPE']
country_continent.to_csv('country_in_countinent.csv', sep  = '|', index=False)



