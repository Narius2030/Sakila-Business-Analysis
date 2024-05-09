import src.hiveconnect.hiveapp as hiveapp

DimInventory=hiveapp.CreateDimInventory('dtptr')
Load_data_to_DimInventory=hiveapp.LoadData("dimInventory.csv","DimInventory",'dtptr')

DimRental=hiveapp.CreateDimRental('dtptr')
Load_data_to_DimRental=hiveapp.LoadData("dimRental.csv",'DimRental','dtptr')

DimDate=hiveapp.CreateTableDimDate('dtptr')
Load_data_to_DimDate=hiveapp.LoadData("dimDate.csv","DimDate",'dtptr')

Fact_Inventory_Analysis_TextFile=hiveapp.CreateTableFact_Inventory_Analysis_TextFile('dtptr')
Load_data_to_Fact_Inventory_TextFile=hiveapp.LoadData("Fact_Inventory_Analysis.csv",'Fact_Inventory_Analysis_TextFile','dtptr')

Fact_Inventory_Analysis_ORC=hiveapp.CreateTableFact_Inventory_Analysis_ORC('dtptr')
