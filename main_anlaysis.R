
install.packages('dplyr')
install.packages('crayon')
install.packages('compositions')
install.packages('zCompositions')
library('zCompositions')
library('dplyr')
library('compositions')

#Q1.what metrics did you chosse for sedentary and sleep, is it the sendentary variables in the excel file.
#Q2
#Q2.only one week or the entire block week 1 and week 2
#Q3. handling the data preprocessing, which way is better?

merged_file <- read.csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit.csv")

colnames(merged_file)

#------------------------calculate the mean and sd of the variables that we will use for the main analysis
#calculate_mean <- c("LPA_week0", "LPA_week2", "LPA_week6", "MVPA_week0", "MVPA_week2", "MVPA_week6",
                   # "Sedentary_week0","Sedentary_week2","Sedentary_week6", "Sleep_week2",  "Sleep_week0", "Sleep_week6",
                   # "MH_baseline", "MH_followup1", "MH_followup2") 

calculate_mean <- names(merged_file)[225:278]
summary_stats <- merged_file %>%
  summarise (
    across(
      all_of(calculate_mean),
      list(
        mean = ~mean(.x, na.rm=TRUE),
        sd = ~sd(.x, na.rm=TRUE)
      )
    )
  )#remove null when I do mean

write.csv(summary_stats, 'summary_stats.csv', row.names=FALSE) 

#-------------------number of goals setting in the dataset










print(colnames(merged_file))


#--------------------------------compositional data anlaysis
#step 1. try baseline composition first
#"LPA_week0", "MVPA_week0", "Sedentary_week0", "Sleep_week0"

baseline_CoDA <- c ("LPA_week0", "MVPA_week0", "Sedentary_week0", "Sleep_week0", "TotalMinutesWearTime_week0")
baseline_CoDA_data <- merged_file[baseline_CoDA]
baseline_rescale <- baseline_CoDA_data %>%
  mutate(
    total = Sleep_week0 + Sedentary_week0 + LPA_week0 + MVPA_week0,
    Sleep_c = Sleep_week0 / total * TotalMinutesWearTime_week0,
    Sed_c  = Sedentary_week0 / total * TotalMinutesWearTime_week0,
    LPA_c  = LPA_week0  / total * TotalMinutesWearTime_week0,
    MVPA_c = MVPA_week0 / total * TotalMinutesWearTime_week0
  )

#replace zeros, CoDA cannot handle zero values

comp_data <- dplyr::select(baseline_rescale, Sleep_c, Sed_c, LPA_c, MVPA_c)
comp_nozeros <- cmultRepl(comp_data, method = "CZM")  # common approach



