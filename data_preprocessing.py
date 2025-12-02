import pandas as pd

#read_file
file1="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\dailyActivity_merged.csv"
file2="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\sleepDay_merged.csv"
file3="C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\fitbitWearTimeViaHR_merged.csv"
survey = "C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Survey_raw_data.csv"

def read_file(file):
    data = pd.read_csv(file)
    return data

#load data
daily_activity = read_file(file1)
sleep_data = read_file(file2)   
wear_time_data = read_file(file3)
survey_data = read_file(survey)

#rename the column name to keep consistent
daily_activity.rename(columns={"ActivityDate":"Date"}, inplace=True)
sleep_data.rename(columns={'SleepDay': 'Date'}, inplace=True) 
wear_time_data.rename(columns={'Day': 'Date'}, inplace=True) 

# ensure `Date` columns are datetimes, then add a formatted string column '%m/%d/%Y'
for df in (daily_activity, sleep_data, wear_time_data):
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')


#combine three files by ID and Activity Date
combined_data1 = pd.merge(daily_activity, sleep_data, on=['Id', 'Date'], how='inner')
combine_final = pd.merge(combined_data1, wear_time_data, on=['Id', 'Date'], how='inner')

#rename ID to number in order to match with the survey data, extract last 3 digits as ID number
combine_final['Id'] = combine_final['Id'].apply(lambda x: int(str(x)[-3:]))
#print("final data:", combine_final_validate2.head())

combine_final_validate1 = combine_final[(combine_final['TotalMinutesWearTime']>600) | (combine_final['TotalSteps'] >1000)] #validate days based on PA; #7722-7677
combine_final_validate2 = combine_final_validate1[(combine_final_validate1['TotalMinutesAsleep']>0)] #asleep needs to be larger than 0
#print(combine_final_validate2.shape) #number of rows and columns

#there is a duplicate excel data we need to process
#133/142 same > none
#152/137 > 152 
#62/65 > 65
#69/72 > 72
#replace 69 in fitbit as 72 
combine_final_validate2.loc[combine_final_validate2['Id'] == 69, 'Id'] = 72

#select the participants who complete health coaching and at least one follow-up survey
#please print the number of consent form; baseline survey; compelete health coaching; complete first follow-up survey; complete second follow-up survey
survey_filtered = survey_data[
    (survey_data['health_coach_survey_complete'] == 2) 
    & (survey_data['weeks_followup_survey_complete'] == 2)
] # 

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
survey_filtered_ids = survey_filtered['record_id'].unique().tolist() #get participant IDs

#select Fitbit data where fitbit_id is in the survey_filtered ids list
fitbit_filter = combine_final_validate2[combine_final_validate2['Id'].isin(survey_filtered_ids)]

#select the fitbit data before completeting the second follow-up survey
for i in survey_filtered_ids:
    finish_date = survey_filtered.loc[survey_filtered['record_id'] == i, 'weeks_followup_survey_6810_timestamp'].values[0]
    fitbit_filter.loc[
        (fitbit_filter['Id'] == i) &
        (pd.to_datetime(fitbit_filter['Date'], format='%m/%d/%Y', errors='coerce') <= pd.to_datetime(finish_date, format='%m/%d/%Y', errors='coerce')),
        'include_in_analysis'
    ] = True

#enlarge the data
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
print("expanded_df shape:", expanded_df['Id'].nunique())
expanded_df.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit.csv", index=False)

print("expand_data", len(expanded_df['Id'].unique())) #n=84

#delete the data points if the number of days is smaller than 25
after_deletion = expanded_df.copy()
for i in expanded_df['Id'].unique():
    id_data = expanded_df[expanded_df['Id'] == i]
    num_days = id_data[id_data['TotalSteps'].notna()].shape[0]
    if num_days < 25:
        after_deletion = after_deletion[after_deletion['Id'] != i]

print("delete_data", len(after_deletion['Id'].unique())) #n=56

#after_deletion.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\expand_fitbit.csv", index=False)

rename_data = after_deletion.copy()
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

#-------------------------prepare the variables we needed and output the long format data

rename_data['MVPA'] = rename_data['VeryActiveMinutes'] + rename_data['FairlyActiveMinutes']
rename_data['Sleep'] = rename_data['TotalMinutesAsleep']
rename_data['Sedentary'] = rename_data['SedentaryMinutes'] # - rename_data['TotalMinutesAsleep']
rename_data['LPA'] = rename_data['LightlyActiveMinutes']

#rename_data.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\fitbit_long_format.csv", index=False)

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
#-----------------------------------------------connvert to long format
fitbit_data_long = rename_data.groupby(['Id','Day'], as_index=False).mean()
final_fitbit_survey_long = pd.merge(fitbit_data_long, replace_outcome_data, left_on='Id', right_on='record_id', how='inner')

final_fitbit_survey_long.to_csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_long.csv", index=False)
#----------------------------------------------convert to wide format
#select variables needed
columns_to_convert = ['TotalSteps','MVPA', 
'Sleep', 
'Sedentary', 
'LPA',
'TotalMinutesWearTime']

wide_data = rename_data[columns_to_convert + ['Id', 'Day']]
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



