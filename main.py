import hiveconnect.hiveapp as hiveapp

# to store the rows_data into list of tuples
data = hiveapp.df_rows_details('employee')

print(data)