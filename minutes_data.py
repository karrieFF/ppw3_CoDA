import pandas as pd
file = "C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\minuteMETsNarrow_merged.csv"
files2 = "C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\Fitbit_raw_data\\heartrate_1min_merged.csv"
file3 = "C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\survey_data\\heartrate_seconds_merged.csv"

fitbit_df = pd.read_csv(file)

print(fitbit_df['METs'].max())