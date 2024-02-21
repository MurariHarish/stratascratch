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

# 5. ID 10176 Bikes Last Used
dc_bikeshare_q1_2012.groupby(by='bike_number')['end_time'].max().reset_index().sort_values('end_time', ascending=False)

# 6. ID 10310 Class Performance
assignment_sum = box_scores.filter(regex='assignment').apply(sum,axis=1)
assignment_sum.max() - assignment_sum.min()

# 7. ID 10308 Salaries Differences
db_merged = pd.merge(db_employee,db_dept, how = 'inner', left_on='department_id', right_on='id')
max_salaries = db_merged.groupby('department')['salary'].max()
abs_difference = abs(max_salaries['marketing'] - max_salaries['engineering'])

# 8. ID 10301 Expensive Projects
emp_proj_count = ms_emp_projects.groupby(by='project_id')['emp_id'].count()
ms_merged = pd.merge(ms_projects, emp_proj_count, how='inner', left_on='id', right_on='project_id')
ms_merged['budget_per_emp'] = round(ms_merged['budget']/ms_merged['emp_id'])
ms_merged[['title','budget_per_emp']].sort_values('budget_per_emp', ascending=False)
 
# 9. ID 10299 Finding Updated Records
ms_employee_salary.groupby(by=['id','first_name','last_name','department_id'])['salary'].max().reset_index().sort_values('id')

# 10 ID 10170 Gender With Most Doctor Appointments
medical_appointments.groupby(by='gender')['appointmentid'].count().reset_index().nlargest(n=1, columns='appointmentid')

# 11 ID 10166 Reviews of Hotel Arena
review_counts = hotel_reviews.groupby(by=['hotel_name','reviewer_score']).agg(n_reviews=('reviewer_score','count')).reset_index()
review_counts[review_counts['hotel_name']=='Hotel Arena']

# 12 ID 10164 Total AdWords Earnings
google_adwords_earnings.groupby('business_type').agg(total_earnings=('adwords_earnings','sum')).reset_index()

# 13 ID 10160 Rank guests based on their ages
airbnb_guests['rank'] = airbnb_guests['age'].rank(method='min', ascending=False)
airbnb_guests[['guest_id', 'rank']].sort_values('rank')

# 14 ID 10156 Number Of Units Per Nationality
airbnb_hosts[airbnb_hosts['age'] < 30].merge(airbnb_units[airbnb_units['unit_type'] == 'Apartment'], how = 'inner',left_on = 'host_id', right_on = 'host_id').drop_duplicates().groupby('nationality').size().reset_index()

# 15 ID 10153 Find the number of Yelp businesses that sell pizza
len(yelp_business[yelp_business['categories'].str.contains('Pizza', case = False)])

# 16 ID 10149 Gender With Generous Reviews
merged_df = pd.merge(airbnb_reviews, airbnb_guests, how = 'inner', left_on = 'from_user', right_on = 'guest_id')
merged_df.groupby(by='gender').agg(review_counts = ('review_score','mean')).reset_index()

# 17 ID 10140 MacBook Pro Events
merged_df = pd.merge(playbook_events,playbook_users, how='inner', on='user_id')
filtered_df = merged_df[(merged_df['language'] != 'Spanish') & (merged_df['device'].str.lower() == 'macbook pro') & (merged_df['location'] == 'Argentina')]
filtered_df.groupby(by=['company_id','language']).size().to_frame('n_events').reset_index()

# 18 ID 10139 Number of Speakers By Language
merged_df = pd.merge(playbook_events,playbook_users, on='user_id', how='inner')
merged_df.groupby(by=['location','language'])['user_id'].nunique().reset_index().sort_values(['location','user_id'])

# 19 ID 10137 Even-numbered IDs Hired in June
worker[(worker['joining_date'].dt.month == 6) & (worker['worker_id']%2 != 1)]
































