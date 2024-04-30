from src.hiveconnect import hiveapp

# Create dimension tables
hiveapp.CreateTableDimRental('buidu')
hiveapp.CreateTableDimCustomer('buidu')

# # Load csv stages to dimension tables
hiveapp.LoadData('dim_rental', 'C:\\Education\\Uni\\BigData\\Final_Project\\Data-Mining-with-ApacheHive\\data\\tables\\dimRental.csv', 'buidu')
hiveapp.LoadData('dim_customer', 'C:\\Education\\Uni\\BigData\\Final_Project\\Data-Mining-with-ApacheHive\\data\\tables\\dimCustomer.csv', 'buidu')

# Create fact table
hiveapp.CreateTableFactSegment('buidu')

# Integrate data to Fact Segment
hiveapp.IntegrateFactSegment('buidu')