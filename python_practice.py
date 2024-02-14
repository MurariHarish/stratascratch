import pandas as pd

# 1. ID 10356 Finding Doctors
def doctors_lname_Johnson (df: pd.DataFrame) -> pd.DataFrame:
    condition = (df['last_name'].str.lower() == 'johnson') & (df['profession'].str.lower() == 'doctor')
    return df.loc[condition, ['first_name', 'last_name']]

doctors_lname_Johnson(df=employee_list)

# 2. ID 10184 Order all countries by the year they first participated in the Olympics
df = olympics_athletes_events.groupby(by = 'noc')['year'].min().reset_index().sort_values(by=['year','noc'])

# 3. ID 10183 Total Cost Of Orders
merged_df = pd.merge(customers, orders, how = 'inner', left_on = 'id', right_on = 'cust_id')
merged_df.groupby(by=['id_x','first_name'])['total_order_cost'].sum().reset_index().sort_values(by='first_name')

# 4. ID 10167 Total Number Of Housing Units
housing_units_completed_us.pivot_table(index='year', values=['south', 'west', 'midwest', 'northeast'], aggfunc='sum').sum(axis=1).reset_index(name='total_num')


