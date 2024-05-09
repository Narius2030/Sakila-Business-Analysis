import src.hiveconnect.hiveapp as hiveapp

DimInventory=hiveapp.CreateDimInventory()
Load_data_to_DimInventory=hiveapp.LoadData("dimInventory.csv","DimInventory")
DimRental=hiveapp.CreateDimRental()
Load_data_to_DimRental=hiveapp.LoadData("dimRental.csv",'DimRental')
DimDate=hiveapp.CreateTableDimDate()
Load_data_to_DimDate=hiveapp.LoadData("dimDate.csv","DimDate")
Fact_Inventory_Analysic=hiveapp.CreateTableFact_Inventory_Analysic()
Load_data_to_Fact_Inventory=hiveapp.LoadData("Fact_Inventory_Analysis.csv",'Fact_Inventory_Analysis')