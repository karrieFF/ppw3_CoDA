import pandas as pd

#read_file
file1="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\dailyActivity_merged.csv"
file2="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\fitbitWearTimeViaHR_merged.csv"
file3="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\sleepStagesDay_merged.csv"
survey = "C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Survey_raw_data.csv"

def read_file(file):
    data = pd.read_csv(file)
    return data

#load data
daily_activity = read_file(file1)
wear_time_data = read_file(file2)
sleep_data = read_file(file3)  
survey_data = read_file(survey)

#rename the column name to keep consistent
daily_activity.rename(columns={"ActivityDate":"Date"}, inplace=True)
wear_time_data.rename(columns={'Day': 'Date'}, inplace=True) 
sleep_data.rename(columns={'SleepDay': 'Date'}, inplace=True) 

# ensure `Date` columns are datetimes, then add a formatted string column '%m/%d/%Y'
for df in (daily_activity, sleep_data, wear_time_data):
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')

#combine three files by ID and Activity Date
combined_data1 = pd.merge(daily_activity, sleep_data, on=['Id', 'Date'], how='inner')
combine_final = pd.merge(combined_data1, wear_time_data, on=['Id', 'Date'], how='inner')

#there is a duplicate excel data we need to process
#133/142 same > none
#152/137 > 152 
#62/65 > 65
#69/72 > 72
#40/40A > 40
#replace 69 in fitbit as 72 
combine_final.loc[combine_final['Id'] == "Y5PPW069", 'Id'] = "Y5PPW072"
combine_final.loc[combine_final['Id'] == "Y5PPW040A", 'Id'] = "Y5PPW040"

#rename ID to number in order to match with the survey data, extract last 3 digits as ID number
combine_final['Id'] = combine_final['Id'].apply(lambda x: int(str(x)[-3:]))
#print("final data:", combine_final_validate2.head())

combine_final_validate1 = combine_final[(combine_final['TotalMinutesWearTime']>600) | (combine_final['TotalSteps'] >1000)] #validate days based on PA; #7722-7677
#combine_final_validate2 = combine_final_validate1[(combine_final_validate1['TotalMinutesAsleep']>0)] #i think we can keep totalminutesasleep=0 first becasue some of them have sufficient PA data; I do not want to delete it

#select the participants who complete health coaching and at least one follow-up survey
#please print the number of consent form; baseline survey; compelete health coaching; complete first follow-up survey; complete second follow-up survey
survey_filtered = survey_data[
    (survey_data['health_coach_survey_complete'] == 2) 
    & (survey_data['weeks_followup_survey_complete'] == 2)
] 

#rescale date format in survey_filtered
rescale_date = ['consent_and_screening_survey_timestamp', 
'baseline_wellness_behavior_survey_timestamp',
'health_coach_survey_timestamp',
'weeks_followup_survey_timestamp',
'weeks_followup_survey_6810_timestamp']

for col in rescale_date:
    survey_filtered[col] = (
        survey_filtered[col]
            .astype(str)              # clean — ensure string
            .str.strip()
            .pipe(pd.to_datetime, format='mixed', errors='coerce')  # proper datetime parsing
            .dt.strftime('%m/%d/%Y')  # now safe
            .fillna("")
    )
#print("Before rescale:", survey_filtered[survey_filtered['record_id'] == 4]['health_coach_survey_timestamp'].values[0])

#unique ID in survey_filtered
survey_filtered_ids = survey_filtered['record_id'].unique().tolist() #get participant IDs: 90

#select Fitbit data where fitbit_id is in the survey_filtered ids list
fitbit_filter = combine_final_validate1[combine_final_validate1['Id'].isin(survey_filtered_ids)]#89

#select the fitbit data before completeting the second follow-up survey
for i in fitbit_filter['Id'].unique():
    finish_date = survey_filtered.loc[survey_filtered['record_id'] == i, 'weeks_followup_survey_6810_timestamp'].values[0]

#if finish_date is None or pd.isna(finish_date), we set finish_date as 4 weeks after the first follow-up survey date
    if finish_date =="":
        finish_date =  pd.to_datetime(survey_filtered.loc[survey_filtered['record_id'] == i, 'weeks_followup_survey_timestamp'].values[0], format='%m/%d/%Y') + pd.Timedelta(weeks=4)

    #print(i, finish_date)
    fitbit_filter.loc[
        (fitbit_filter['Id'] == i) &
        (pd.to_datetime(fitbit_filter['Date'], format='%m/%d/%Y', errors='coerce') <= pd.to_datetime(finish_date, format='%m/%d/%Y', errors='coerce')),
        'include_in_analysis'
    ] = True

#fitbit_filter.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_filtered.csv", index=False)

#------------------------------------Approach 1: use health coach as reference. ------------------------------------------------
enlarge_data = fitbit_filter.copy()

expanded_list = []

for uid, sub in enlarge_data.groupby('Id'):

    base_date = survey_filtered.loc[survey_filtered['record_id'] == uid, 'health_coach_survey_timestamp'].values[0]

    end = pd.to_datetime(base_date, errors='coerce') + pd.Timedelta(weeks=6)
    start = pd.to_datetime(base_date, errors='coerce') - pd.Timedelta(weeks=1)

    sub["Date"] = pd.to_datetime(sub["Date"], errors="coerce")
    #start = sub['Date'].min()
    #print('uid', uid, 'start', start, 'end', end  )
    
    #build full daily index
    full_range = pd.DataFrame({
        'Date': pd.date_range(start, end),
        'Id': uid
    })

    full_range['Date'] = pd.to_datetime(full_range['Date'], errors='coerce')
    sub['Date'] = pd.to_datetime(sub['Date'], errors='coerce')

    merged = full_range.merge(sub, on=['Id','Date'], how='left')
    #print('full_range', uid, full_range.shape, sub.shape, merged[merged['TotalSteps'].notna()].shape)
    expanded_list.append(merged)
    # print('meged',uid, merged.shape)

expanded_df = pd.concat(expanded_list, ignore_index=True)
#print("expanded_df shape:", expanded_df['Id'].nunique())
#expanded_df.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit_v1.csv", index=False)

print("expand_data", len(expanded_df['Id'].unique())) #n=84

#step 2: rename to another column
rename_data = expanded_df.copy()
all_list = []

#rename date to another column
for i in rename_data['Id'].unique():
    id_data = rename_data[rename_data['Id'] == i]
    id_data = id_data.sort_values(by='Date')
    id_data['Date'] = pd.to_datetime(id_data['Date'], errors='coerce')

    #coaching date
    coaching_date = survey_filtered.loc[survey_filtered['record_id'] == i, 'health_coach_survey_timestamp'].values[0]
    coaching_date = pd.to_datetime(coaching_date, errors='coerce')

    #default: before coaching day
    id_data['Day']='week0'

    #exact coaching day
    id_data.loc[id_data['Date'] == coaching_date, 'Day'] = 'coaching_day'

    #after coaching day
    after_mask = id_data['Date'] > coaching_date
    days_after = (id_data.loc[after_mask, 'Date'] - coaching_date).dt.days
    weeks_after = ((days_after-1) // 7) + 1
    weeks_after = weeks_after.clip(upper=7)  # Cap at week 6

    id_data.loc[after_mask, 'Day'] = 'week' + weeks_after.astype(str)

    all_list.append(id_data)

# put all ids back together
rename_data = pd.concat(all_list, ignore_index=True)

# step 3: Group by and also calculate the sample size per week
fitbit_data_long1 = rename_data.groupby(['Id','Day'], as_index=False).mean() #Group by and calculate mean by

measre_cols = [c for c in rename_data.columns 
               if c not in ['Id', 'Day', 'Date']]

wanted_days = ['coaching_day', 'week0', 'week1',
               'week2', 'week3', 'week4', 'week5', 'week6', 'others']

sub = rename_data[rename_data['Day'].isin(wanted_days)].copy()

sub['valid_day'] = sub[measre_cols].notna().any(axis=1)

n_valid_per_day1 = (
    sub.groupby(['Id', 'Day'])['valid_day']
       .sum()
       .reset_index(name='n_valid_days')
)

fitbit_data_long1 = pd.merge(fitbit_data_long1, n_valid_per_day1, on=['Id', 'Day'], how='left')# 3. Merge counts into the mean table

# #Step 3: merge them by week

# fitbit_data_long12 = fitbit_data_long1.copy()
# fitbit_data_long12['Day'] = fitbit_data_long12['Day'].replace({
#     'week1': 'week2',
#     'week3': 'week6',
#     'week4': 'week6',
#     'week5': 'week6'
# })
# fitbit_data_long12_v2 = fitbit_data_long12.groupby(['Id','Day'], as_index=False).mean()

#step 4: decide the deletion > Do we delete?
#delete the data points if the number of days is smaller than 25 (Do not th)
# after_deletion = expanded_df.copy()
# for i in expanded_df['Id'].unique():
#     id_data = expanded_df[expanded_df['Id'] == i]
#     num_days = id_data[id_data['TotalSteps'].notna()].shape[0]
#     if num_days < 25:
#         after_deletion = after_deletion[after_deletion['Id'] != i]

# print("delete_data", len(after_deletion['Id'].unique())) #n=56

#fitbit_data_long1.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_data_long_v1.csv", index=False)
#fitbit_data_long12_v2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_data_long_v1_2.csv", index=False)

#rename_data.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_long_format.csv", index=False)

#------------------------------------Approach 2: use the first day of using Fitbit as reference------------------------------------------------
#---step1: expand the data based on the first day of using Fitbit and the last day of completing the second follow-up survey
enlarge_data2 = fitbit_filter.copy()

expanded_list2 = []
for uid, sub in enlarge_data.groupby('Id'):
    end = survey_filtered.loc[survey_filtered['record_id'] == uid, 'weeks_followup_survey_6810_timestamp'].values[0]
    if end =="":
        end =  pd.to_datetime(survey_filtered.loc[survey_filtered['record_id'] == uid, 'weeks_followup_survey_timestamp'].values[0], format='%m/%d/%Y') + pd.Timedelta(weeks=4)

    sub["Date"] = pd.to_datetime(sub["Date"], errors="coerce")
    
    start = sub['Date'].min()

    #build full daily index
    full_range = pd.DataFrame({
        'Date': pd.date_range(start, end),
        'Id': uid
    })

    full_range['Date'] = pd.to_datetime(full_range['Date'], errors='coerce')
    sub['Date'] = pd.to_datetime(sub['Date'], errors='coerce')

    merged = full_range.merge(sub, on=['Id','Date'], how='left')

    expanded_list2.append(merged)

expanded_df2 = pd.concat(expanded_list2, ignore_index=True)
#expanded_df2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit_approach2_raw.csv", index=False)
#--step 2: rename to another column 
rename_data2 = expanded_df2.copy()

all_list2 = []
for i in rename_data2['Id'].unique():
    id_data2 = rename_data2[rename_data2['Id']==i]
    id_data2 = id_data2.sort_values(by='Date') #sort the data
    id_data2['Date'] = pd.to_datetime(id_data2['Date'], errors='coerce') #convert to date format

    health_coaching_date = pd.to_datetime(survey_filtered.loc[survey_filtered['record_id'] == i, 'health_coach_survey_timestamp'].values[0], errors='coerce')
    follow1 = pd.to_datetime(survey_filtered.loc[survey_filtered['record_id'] == i, 'weeks_followup_survey_timestamp'].values[0], errors='coerce')
    follow2 =pd.to_datetime(survey_filtered.loc[survey_filtered['record_id'] == i, 'weeks_followup_survey_6810_timestamp'].values[0])
    if pd.isna(follow2):
        follow2 = follow1 + pd.Timedelta(weeks=4)   
    
    id_data2['Day']='others'

    #extract baseline, coaching date, follow1 and follow2

    id_data2.loc[
    ((id_data2['Date'] > health_coaching_date - pd.Timedelta(weeks=1) - pd.Timedelta(days=1)) & (id_data2['Date'] < health_coaching_date)),
    'Day'
    ] = 'baseline'

    id_data2.loc[
        (id_data2['Date'] > follow1 - pd.Timedelta(weeks=2) - pd.Timedelta(days=1)) & (id_data2['Date'] < follow1 - pd.Timedelta(weeks=1)),
        'Day'
    ] = 'followupweek1'

    id_data2.loc[
        (id_data2['Date'] > follow1 - pd.Timedelta(weeks=1) - pd.Timedelta(days=1)) & (id_data2['Date'] < follow1),
        'Day'
    ] = 'followupweek2'

    id_data2.loc[
        (id_data2['Date'] > follow2 - pd.Timedelta(weeks=2) - pd.Timedelta(days=1)) & (id_data2['Date'] < follow2-pd.Timedelta(weeks=1)),
        'Day'
    ] = 'followupweek5'

    id_data2.loc[
        (id_data2['Date'] > follow2 - pd.Timedelta(weeks=1) - pd.Timedelta(days=1)) & (id_data2['Date'] < follow2),
        'Day'
    ] = 'followupweek6'

    all_list2.append(id_data2)  

# put all ids back together
rename_data2 = pd.concat(all_list2, ignore_index=True)
#rename_data2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit_approach2.csv", index=False)

#---step3: Group by and calculate both mean and Size

fitbit_data_long2 = rename_data2.groupby(['Id', 'Day'], as_index=False).mean() # 1. mean per Id–Day

measre_cols = [c for c in rename_data2.columns 
               if c not in ['Id', 'Day', 'Date']]

wanted_days = ['baseline', 'followupweek1', 'followupweek2',
               'followupweek5', 'followupweek6', 'others']

sub = rename_data2[rename_data2['Day'].isin(wanted_days)].copy()

sub['valid_day'] = sub[measre_cols].notna().any(axis=1)

n_valid_per_day = (
    sub.groupby(['Id', 'Day'])['valid_day']
       .sum()
       .reset_index(name='n_valid_days')
)

#print(n_valid_per_day)
fitbit_data_long2 = fitbit_data_long2.merge(n_valid_per_day, on=['Id', 'Day'], how='left')# 3. Merge counts into the mean table
#fitbit_data_long2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit_approach2.csv", index=False)

#---step 4: delete the data points
#A day is valid if it has any non-NAN measure
# sub['valid_day'] = sub[measre_cols].notna().any(axis=1)

# # id is valid if it at least one required day is valid
# valid_ids = sub.groupby('Id') ['valid_day'].any()

# #keep only valid ids
# fitbit_long2_clean = fitbit_data_long2[fitbit_data_long2['Id'].isin(valid_ids[valid_ids].index)]

# print('valid_ids', fitbit_long2_clean['Id'].nunique()) #n=77, delete 7 participants
#fitbit_data_long2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_data_long_v2.csv", index=False)

#---------------------------prepare survey outcomes

#print('final_data', final_data.columns.tolist())
#baseline: nervous/down/blue 1=0, 2=25, 3=50, 4=75, 5=100; clam/happy: 1=100, 2=75, 3=50, 4=25, 5=0
#follow-up: nervous/down/blue 1=0, 2=20, 3=40, 4=60, 5=80, 6=100; calm/happy: 1=100, 2=80, 3=60, 4=40, 5=0
replace_outcome_data = survey_filtered.copy()

#baseline
replace_outcome_data['nervous'] = replace_outcome_data['nervous'].replace({1:0, 2:25, 3:50, 4:75, 5:100})
replace_outcome_data['down'] = replace_outcome_data['nervous'].replace({1:0, 2:25, 3:50, 4:75, 5:100})
replace_outcome_data['blue'] = replace_outcome_data['down'].replace({1:0, 2:25, 3:50, 4:75, 5:100})
replace_outcome_data['calm'] = replace_outcome_data['calm'].replace({1:100, 2:75, 3:50, 4:25, 5:0})
replace_outcome_data['happy'] = replace_outcome_data['happy'].replace({1:100, 2:75, 3:50, 4:25, 5:0})

#follow-up 1
replace_outcome_data['nervous_v1'] = replace_outcome_data['nervous_v1'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['down_v1'] = replace_outcome_data['down_v1'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['blue_v1'] = replace_outcome_data['blue_v1'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['calm_v1'] = replace_outcome_data['calm_v1'].replace({1:100, 2:80, 3:60, 4:40, 5:20, 6:0})
replace_outcome_data['happy_v1'] = replace_outcome_data['happy_v1'].replace({1:100, 2:80, 3:60, 4:40, 5:20, 6:0})

#follow-up 2
replace_outcome_data['nervous_v2'] = replace_outcome_data['nervous_v2'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['down_v2'] = replace_outcome_data['down_v2'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['blue_v2'] = replace_outcome_data['blue_v2'].replace({1:0, 2:20, 3:40, 4:60, 5:80, 6:100})
replace_outcome_data['calm_v2'] = replace_outcome_data['calm_v2'].replace({1:100, 2:80, 3:60, 4:40, 5:20, 6:0})
replace_outcome_data['happy_v2'] = replace_outcome_data['happy_v2'].replace({1:100, 2:80, 3:60, 4:40, 5:20, 6:0})

#aggregate MH score
replace_outcome_data['MH_baseline'] = replace_outcome_data[['nervous', 'down', 'blue', 'calm', 'happy']].mean(axis=1)
replace_outcome_data['MH_followup1'] = replace_outcome_data[['nervous_v1', 'down_v1', 'blue_v1', 'calm_v1', 'happy_v1']].mean(axis=1)
replace_outcome_data['MH_followup2'] = replace_outcome_data[['nervous_v2', 'down_v2', 'blue_v2', 'calm_v2', 'happy_v2']].mean(axis=1)
#print("replace_outcome_data", replace_outcome_data[['record_id', 'MH_baseline', 'MH_followup1', 'MH_followup2']].head())

#print("After rescale:", replace_outcome_data[['consent_and_screening_survey_timestamp', 'baseline_wellness_behavior_survey_timestamp']])
#----------------------------------------------Apporach 1: Combine with the survey data in long format

final_fitbit_survey_long1 = pd.merge(fitbit_data_long1, replace_outcome_data, left_on='Id', right_on='record_id', how='inner')
final_fitbit_survey_long1.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_long_v1.csv", index=False)

#-----------------------------------------------Approach 2: combine with the survey data in long format

final_fitbit_survey_long2 = pd.merge(fitbit_data_long2, replace_outcome_data, left_on='Id', right_on='record_id', how='inner')
final_fitbit_survey_long2.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_long_v2.csv", index=False)

#----------------------------------------------Approach 1: convert to wide format------------------------------------------------------------------
#prepare the variables we needed and output the long format data

final_fitbit_survey_long1['MVPA'] = final_fitbit_survey_long1['VeryActiveMinutes'] + final_fitbit_survey_long1['FairlyActiveMinutes']
final_fitbit_survey_long1['Sleep'] = final_fitbit_survey_long1['TotalMinutesAsleep']
final_fitbit_survey_long1['Sedentary'] = final_fitbit_survey_long1['SedentaryMinutes'] # - rename_data['TotalMinutesAsleep']
final_fitbit_survey_long1['LPA'] = final_fitbit_survey_long1['LightlyActiveMinutes']


#select variables needed
columns_to_convert = ['TotalSteps','MVPA', 
'Sleep', 
'Sedentary', 
'LPA',
'TotalMinutesWearTime']

wide_data = final_fitbit_survey_long1[columns_to_convert + ['Id', 'Day']]
#wide_data.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit.csv", index=False)

#convert to wide format
weekly = (
    wide_data
    .groupby(['Id','Day'], as_index=False)[columns_to_convert]
    .mean()
)

day_counts = wide_data.groupby(['Id', 'Day'])['LPA'].count()
day_counts = day_counts.rename('n_days')        # give the series a name
day_counts = day_counts.reset_index()           # convert to DataFrame
#print(day_counts)

weekly = pd.merge(weekly, day_counts, on=['Id','Day'], how='left'
)
wide_all = (
    weekly
    .set_index(['Id', 'Day'])
    .unstack('Day')           # Day values become columns
)

# flatten MultiIndex columns: ('TotalMinutesAsleep','week1') → 'TotalMinutesAsleep_week1'
wide_all.columns = [
    f"{var}_{day}" for var, day in wide_all.columns.to_flat_index()
]

wide_all = wide_all.reset_index()

#merge with survey data
final_data_wide = pd.merge(replace_outcome_data, wide_all, left_on='record_id', right_on='Id', how='inner')

#export the final data
#final_data.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit.csv", index=False)

final_data_wide.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_wide.csv", index=False)



