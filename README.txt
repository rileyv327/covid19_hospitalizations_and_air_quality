This project looks at the relationship between air quality and covid_19 over time in NYC.


This file contains purely what code and programs were used to come up with the analytic at 
a high level. This project was run entirely using MapReduce jobs or Hive queries, then exporting
to csv's for visuals.

A breif abstract of the research:
After living through the COVID-19 pandemic and seeing firsthand how one disease could permanently 
alter the course of everyone’s lives, I wondered if there were any underlying factors that contributed
to one’s likelihood of contracting COVID-19.

As a disease affecting the respiratory system, I naturally questioned whether the particles in the air 
could impact one’s chances. The overall purpose of this research was to determine if there was any 
correlation between COVID-19 hospitalizations and an environment’s air quality. More specifically, 
I studied the relationship between PM2.5 levels and COVID-19 hospitalizations in New York City. I found
two data sources, one containing COVID-19 hospitalization data and another containing PM2.5 readings. 
The datasets were then uploaded to HDFS on the Apache Hadoop ecosystem, cleaned the data using MapReduce 
software, and finally the cleaned data was uploaded to Hive for analysis.

During the Hive analysis, I mainly looked at hospitalizations above and below average air quality for an 
area and took into account some key dates of interest. I found no significant correlation between COVID-19 
hospitalizations and PM2.5 levels in New York City. Though, when looking at the data, we can see many 
possible confounding variables (i.e. mask mandates and vaccine availability) that more severely impact 
hospitalizations, leaving it close to impossible to see any real correlation with air quality.


The full write-up can be found here:
https://medium.com/@riley.valls327/analysis-of-covid-19-hospitalizations-in-relation-to-air-quality-pm2-5-4d41a84a7767