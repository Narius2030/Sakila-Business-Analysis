from src.hiveconnect import hiveapp

# Create dimension tables
hiveapp.CreateTableDimRental(username='buidu')
hiveapp.CreateTableDimCustomer(username='buidu')

# # Load csv stages to dimension tables
hiveapp.LoadData('dimRental.csv', 'dim_rental', username='buidu')
hiveapp.LoadData('dimCustomer.csv', 'dim_customer', username='buidu')

# Create fact table
hiveapp.CreateTableFactSegment(username='buidu')

# Integrate data to Fact Segment
hiveapp.IntegrateFactSegment(username='buidu')