import pandas as pd
# Medium Questions

# 1 ID 10352 Users By Average Session Time
facebook_page_load = facebook_web_log[facebook_web_log['action'] == 'page_load']
facebook_page_load['day'] = facebook_page_load['timestamp'].dt.date
facebook_page_load.groupby(['user_id','day'], as_index=False).max()
facebook_page_exit = facebook_web_log[facebook_web_log['action'] == 'page_exit']
facebook_page_exit['day'] = facebook_page_exit['timestamp'].dt.date
facebook_page_exit.groupby(['user_id','day'], as_index=False).min()
merged_df = pd.merge(facebook_page_load,facebook_page_exit,how='inner',on=['user_id','day'])
merged_df['diff'] = merged_df['timestamp_y'] - merged_df['timestamp_x']
merged_df.groupby('user_id')['diff'].mean().dropna().reset_index()

# 2 ID 10351 Activity Rank
gmails_groupby = google_gmail_emails.groupby('from_user').agg(n_emails = ('to_user','count')).reset_index()
gmails_groupby['rank'] = gmails_groupby['n_emails'].rank(method = 'first',ascending=False)
gmails_groupby.sort_values(by=['n_emails', 'from_user'], ascending=[False, True])

# 3 ID 10324 Distances Traveled
lyft_user_merge = pd.merge(lyft_rides_log,lyft_users,how='inner',left_on='user_id',right_on='id')
lyft_user_by_distance = lyft_user_merge.groupby(['user_id','name'])['distance'].sum().reset_index()
lyft_user_by_distance['rank'] = lyft_user_by_distance['distance'].rank(ascending=False)
top_10=lyft_user_by_distance[lyft_user_by_distance['rank']<= 10].sort_values('rank',ascending=True)
top_10.iloc[:,:-1]

# 4 ID 10322 Finding User Purchases
amazon_transactions['created_at'] = pd.to_datetime(amazon_transactions['created_at']).dt.strftime('%m-%d-%Y')
df = amazon_transactions.sort_values(['user_id','created_at'], ascending=[True,True])
df['prev_value'] = df.groupby('user_id')['created_at'].shift()
df['days_diff'] = (pd.to_datetime(df['created_at']) - pd.to_datetime(df['prev_value'])).dt.days
df[df['days_diff'] < 7]['user_id'].unique()

# 5 ID 10318 New Products
car_launch_groupby = car_launches.groupby(['company_name','year'])['product_name'].count().reset_index()
car_launch_pivot = car_launch_groupby.pivot_table(index = 'company_name', columns='year', values = 'product_name').reset_index()
car_launch_pivot['net_diff'] = car_launch_pivot[2020] - car_launch_pivot[2019]
car_launch_pivot[['company_name','net_diff']]

# 6 ID 10366 Customer Feedback Analysis
customer_feedback[(customer_feedback['source_channel'] == 'social_media') & (customer_feedback['comment_category']!= 'short_comments')].drop_duplicates()

# 7 ID 10364 Friday's Likes Count
reverse_friendships = friendships.rename(columns={'user_name1': 'user_name2', 'user_name2': 'user_name1'})
bidirectional_friendships = pd.concat([friendships, reverse_friendships], ignore_index=True).drop_duplicates()
post_likes_df = pd.merge(user_posts,likes,how='inner',on=['post_id'])
post_likes_friends_df = pd.merge(post_likes_df,bidirectional_friendships,how='inner',left_on=['user_name_x','user_name_y'],right_on=['user_name1','user_name2'])
post_likes_friends_df['weekday'] = post_likes_friends_df['date_liked'].dt.day_name()
post_likes_friends_df[post_likes_friends_df['weekday'] == 'Friday'].groupby('date_liked').size().reset_index(name='likes')

# 8 ID 10315 Cities With The Most Expensive Homes
average_home_price = pd.DataFrame(zillow_transactions.groupby('city')['mkt_price'].mean().reset_index(name = 'city_avg_price'))
zillow_transactions['national_avg'] = zillow_transactions['mkt_price'].mean()
merged_df = pd.merge(average_home_price,zillow_transactions,how='inner',on='city')
merged_df[merged_df['city_avg_price'] > merged_df['national_avg']]['city'].drop_duplicates()

# 9 ID 10304 Risky Projects
linkedin_projects['project_days'] = (pd.to_datetime(linkedin_projects['end_date']) - pd.to_datetime(linkedin_projects['start_date'])).dt.days
linkedin_employees['per_day_salary'] = (linkedin_employees['salary'] / 365).round()
emp_projects_df = pd.merge(linkedin_employees,linkedin_emp_projects,how='inner',left_on='id',
right_on='emp_id')
project_emp_salary = emp_projects_df.groupby('project_id')['per_day_salary'].sum().reset_index()
final_project_emp_df = pd.merge(linkedin_projects,project_emp_salary,how='inner',left_on='id',right_on='project_id')
final_project_emp_df['project_emp_salary'] = final_project_emp_df['per_day_salary'] * final_project_emp_df['project_days']
final_project_emp_df[final_project_emp_df['budget'] < final_project_emp_df['project_emp_salary']][['title','budget','project_emp_salary']]

# 10 ID 10303 Top Percentile Fraud
fraud_score['percentile'] = fraud_score.groupby('state')['fraud_score'].rank(pct=True)
fraud_score[fraud_score['percentile']>0.95].iloc[:,:-1]




# Easy Questions

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

# 20 ID 10136 Odd-numbered ID's Hired in February
worker['joining_month'] = worker['joining_date'].dt.month
worker[(worker['joining_month'] == 2) & worker['worker_id'] % 2 == 1].drop(['joining_month'], axis=1)

# 21 ID 10128 Count the number of movies that Abigail Breslin nominated for oscar
len(oscar_nominees[oscar_nominees['nominee'].str.lower() == 'abigail breslin'])

# 22 ID 10127 Calculate Samantha's and Lisa's total sales revenue
filtered_df = sales_performance[sales_performance['salesperson'].str.lower().isin(['samantha','lisa'])]
filtered_df['sales_revenue'].sum()

# 23 ID 10087 Find all posts which were reacted to with a heart
heart_post_id_list = list(facebook_reactions[facebook_reactions['reaction'].str.lower()=='heart']['post_id'].unique())
facebook_posts[facebook_posts['post_id'].isin(heart_post_id_list)]

# 24 ID 10078 Find matching hosts and guests in a way that they are both of the same gender and nationality
pd.merge(airbnb_hosts,airbnb_guests,how='inner', on=['nationality','gender'])[['host_id','guest_id']].drop_duplicates()

# 25 ID 10020 Find prices for Spanish, Italian, and French wines
countries = ['Spain','Italy','France']
winemag_p1[winemag_p1['country'].isin(countries)][['price']]

# 26 ID 10022 Find all wine varieties which can be considered cheap based on the price
winemag_p1.query('5 < price <= 20')[['variety']].drop_duplicates()

# 27 ID 2004 Number of Comments Per User in 30 days before 2020-02-10
fb_comments_count[(fb_comments_count['created_at'] >= pd.to_datetime('2020-02-10') - timedelta(days=30)) & (fb_comments_count['created_at'] <= pd.to_datetime('2020-02-10'))].groupby('user_id').sum().reset_index()

# 28 ID 2005 Share of Active Users
result = fb_active_users[fb_active_users['country'] == 'USA'].groupby('status')['user_id'].count().to_frame(
    'user_count')
result = result.loc['open'] / result['user_count'].sum()

# 29 ID 2002 Submission Types
result1 = loans[loans['type'] == 'Refinance']
result2 = loans[loans['type'] == 'InSchool']
result = pd.merge(result1,result2,on='user_id',how='inner')[['user_id']].drop_duplicates()




















































