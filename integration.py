import src.hiveconnect.hiveapp as hiveapp
username='dtptr'
DimInventory=hiveapp.CreateDimInventory(username)
Load_data_to_DimInventory=hiveapp.LoadData("dimInventory.csv","DimInventory",username)

DimRental=hiveapp.CreateDimRental(username)
Load_data_to_DimRental=hiveapp.LoadData("dimRental.csv",'DimRental',username)

DimDate=hiveapp.CreateTableDimDate(username)
Load_data_to_DimDate=hiveapp.LoadData("dimDate.csv","DimDate",username)

Fact_Inventory_Analysis_TextFile=hiveapp.CreateTableFact_Inventory_Analysis_TextFile(username)
Load_data_to_Fact_Inventory_TextFile=hiveapp.LoadData("Fact_Inventory_Analysis.csv",'Fact_Inventory_Analysis_TextFile',username)

Fact_Inventory_Analysis_ORC=hiveapp.CreateTableFact_Inventory_Analysis_ORC(username)
