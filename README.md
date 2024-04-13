# Demand Navigator Application

This is a real time data engineering project for guiding a cab company drivers towards areas of the cities with higher trip requests. It is built using below mentioned Azure services.

1) Azure event hub
2) Azure data lake storage gen2
3) Azure databricks
5) Tableau

The Architecture Diagram for this project is shown below - 
</br>

<img src=./images/Demand_Navigator_Architecture.jpg width="700" height="500">

</br>
The main tasks involved are - 

</br>
</br>

1) Generating data similar to trip requests using streaming_data_generator.py
2) sending the generated data to Event Hubs using connection string.
3) Storing logical regions of the city as reference data in Azure data lake storage gen2.
4) Importing and transforming live trip requests data using geographic_data.csv with the help of sliding window and watermarking in spark structured streaming.
5) Displaying the results in tableau dashboards using live connection to databricks. 

</br>

</br>

I have used the below mentioned resources in Azure portal for building this movie recommender project end-to-end.
</br>
1) Event Hubs
2) Azure Databricks Service
3) Data factory (V2)
4) Storage account
6) Tableau
</br>
<img src=./images/rg.jpeg width="700" height="500">



