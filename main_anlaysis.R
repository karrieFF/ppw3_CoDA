
#Q1.what metrics did you chosse for sedentary and sleep, is it the sendentary variables in the excel file.
#Q2.only one week or the entire block week 1 and week 2
#Q3. handling the data preprocessing, which way is better?
#Q4. maybe do not use wear time because it is not really total minutes when doing the convert, how to do 
#Q5, how to solve the wear time

#---------------load packages
install.packages('dplyr')
install.packages('crayon')
install.packages('compositions')
install.packages('zCompositions')
install.packages(c("ggtern", "dplyr"))
install.packages('tidyr')
install.packages("lme4")

library('zCompositions')
library('dplyr')
library('compositions')
library('ggtern')
library('dplyr')
library('tidyr')
library('lme4')

merged_file <- read.csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_wide.csv")
fitbit_long_form <- read.csv("C:\\Users\\flyka\\Box\\PPW3_compositional_analysis\\merge_survey_fitbit_long.csv")
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
print(colnames(merged_file))

#--------------------------------compositional data anlaysis
#step 1. try baseline composition first
#"LPA_week0", "MVPA_week0", "Sedentary_week0", "Sleep_week0"
baseline_CoDA <- c ("LPA_week0", "MVPA_week0", "Sedentary_week0", "Sleep_week0", "TotalMinutesWearTime_week0",
                    "LPA_week1", "MVPA_week1", "Sedentary_week1", "Sleep_week1", "TotalMinutesWearTime_week1",
                    "LPA_week2", "MVPA_week2", "Sedentary_week2", "Sleep_week2", "TotalMinutesWearTime_week2",
                    "MH_baseline",
                    "MH_followup1",
                    "MH_followup2",
                    "demographics_sex")

baseline_CoDA_data <- merged_file[baseline_CoDA]
baseline_rescale <- baseline_CoDA_data %>%
  mutate(
    total_0 = Sleep_week0 + Sedentary_week0 + LPA_week0 + MVPA_week0,
    Sleep_0 = Sleep_week0 / total_0 * 1440, #here can be 100
    Sed_0  = Sedentary_week0 / total_0 * 1440,
    LPA_0  = LPA_week0  / total_0 * 1440,
    MVPA_0 = MVPA_week0 / total_0 * 1440,
    total_1 = Sleep_week1 + Sedentary_week1 + LPA_week1 + MVPA_week1,
    Sleep_1 = Sleep_week1 / total_1 * 1440,
    Sed_1  = Sedentary_week1 / total_1 * 1440,
    LPA_1  = LPA_week1  / total_1 * 1440,
    MVPA_1 = MVPA_week1 / total_1 * 1440,
    total_2 = Sleep_week2 + Sedentary_week2 + LPA_week2 + MVPA_week2,
    Sleep_2 = Sleep_week2 / total_2 * 1440,
    Sed_2  = Sedentary_week2 / total_2 * 1440,
    LPA_2  = LPA_week2  / total_2 * 1440,
    MVPA_2 = MVPA_week2 / total_2 * 1440,
  ) #maybe do not use wear time because it is not really total minutes,

#replace zeros, CoDA cannot handle zero values
#summary(baseline_rescale)
#sapply(baseline_rescale, function(x) sum(x == 0, na.rm = TRUE)) #check if there is 0, if all of them is zero, that means there is no value is 0

#comp_data <- dplyr::select(baseline_rescale, Sleep_c, Sed_c, LPA_c, MVPA_c)
#comp_nozeros <- cmultRepl(comp_data, method = "CZM")  # common approach

#-----------------------prepare the data 
#-convert to an ordered composition
select0 <- c('MVPA_0', 'Sed_0', 'LPA_0', 'Sleep_0')
select1 <- c('MVPA_1', 'Sed_1', 'LPA_1', 'Sleep_1')
select2 <- c('MVPA_2', 'Sed_2', 'LPA_2', 'Sleep_2')

data_baseline <- baseline_rescale[,select0]
data_follow1 <- baseline_rescale[,select1]
data_follow2 <- baseline_rescale[,select2]

comp_acomp0 <- acomp(data_baseline)
comp_acomp1 <- acomp(data_follow1)
comp_acomp2 <- acomp(data_follow2)

#-----------------Calculate geometric (compositional) mean, you can rebuilt the coordinator by adjust the sequence of the variables or create your own matrix
comp_mean0 <- mean(comp_acomp0)   # this is the compositional mean
comp_mean_min0 <- as.numeric(comp_mean0) * 1440  #convert to 1440 
comp_mean_per0 <- as.numeric(comp_mean0) * 100 #convert to percentage

comp_mean1 <- mean(comp_acomp1)   # this is the compositional mean
comp_mean_min1 <- as.numeric(comp_mean1) * 1440  #convert to 1440 
comp_mean_per1 <- as.numeric(comp_mean1) * 100 #convert to percentage

comp_mean1 <- mean(comp_acomp1)   # this is the compositional mean
comp_mean_min0 <- as.numeric(comp_mean1) * 1440  #convert to 1440 
comp_mean_per1 <- as.numeric(comp_mean1) * 100 #convert to percentage

#--------------------------------calculate variation matrics
variation_matrix <- variation(comp_acomp)
variation_matrix

#--------------------------------Ternary plots
#step 1. prepare the data
select_variables <- c('Day', 'MVPA', 'Sleep', 'Sedentary', 'LPA')
select_variables_data <- fitbit_long_form[,select_variables]

rescale_data <- select_variables_data %>%
  filter(Day %in% c('week0', 'week2')) %>%
  mutate(
    total = MVPA + Sedentary + LPA,
    Sleepv = Sleep / total, #here can be 100
    Sedentaryv  = Sedentary / total,
    LPAv  = LPA  / total,
    MVPAv = MVPA / total,
  ) #maybe d #0-1

#step 2 replace zeros with a tiny value
rescale_data <- rescale_data %>%
  mutate(across(c(MVPAv, Sedentaryv, LPAv),
              ~ ifelse(. <= 0, 1e-6, .))) %>%
  # re-close so they sum exactly to 1
  mutate(
    sum3 = MVPAv + Sedentaryv + LPAv,
    MVPAv      = MVPAv      / sum3,
    Sedentaryv = Sedentaryv / sum3,
    LPAv       = LPAv       / sum3
  )

#step 3 plot the graph
Pa <- ggtern(rescale_data, aes(x = MVPAv, y = Sedentaryv, z = LPAv, color = Day)) +
  geom_point(size = 2, alpha = 0.8) +
  geom_confidence_tern(
    aes(group = Day, color = Day),
    breaks = 0.95,
    n = 500
  ) +
  scale_color_manual(values = c(
    "week0" = "goldenrod",   # yellow
    "week2" = "brown"        # brown
  )) +
  theme_bw() +
  theme_showarrows() +
  labs(
    title = "Baseline vs Week2",
    T = "SED",
    L = "MVPA",
    R = "LPA"
  )
Pa

#---------------------------compare the difference between two compostional anlaysis
select_week0 <- select_variable_week02[select_variable_week02$Day %in% 'week0', ]
select_week2 <- select_variable_week02[select_variable_week02$Day %in% 'week2', ]

com_week0 <- acomp(select_week0)
com_week2 <- acomp(select_week2)

#step 2. calculate the difference
diff_comp <- com_week2 - com_week0 #calculate difference
diff_df <- as.data.frame(diff_comp) #convert to dataframe

#------------------regression analysis
colnames(fitbit_long_form)
CoDA_var <- c ("Id","Day", "MVPA", "Sleep", "Sedentary", "LPA", "MH_baseline","MH_followup1", "MH_followup2", "demographics_sex")
CoDA_dat <- fitbit_long_form[,CoDA_var]
CoDA_rescale <- CoDA_dat %>%
  mutate(
    total_v = Sleep + Sedentary + LPA + MVPA,
    Sleep = Sleep / total_v * 1440, #here can be 100
    Sedentary  = Sedentary / total_v * 1440,
    LPA  = LPA  / total_v * 1440,
    MVPA = MVPA / total_v * 1440,
  ) #maybe do not use wear time because it is not really total minutes,

#convert mental health
CoDA_mh_long <- CoDA_rescale %>% 
  pivot_longer(
    cols = c(MH_baseline, MH_followup1, MH_followup2),
    names_to = 'timepoint',
    values_to = 'MH_Score'
  )

#----------------select week0, week 2, and week 6
CoDA_select <- CoDA_mh_long[CoDA_mh_long$Day %in% c('week0', 'week2', 'week6'),]
coDA_comp <- acomp(CoDA_select[,c("MVPA", "Sleep", "Sedentary","LPA")])

#transform to log ratios #4parts convert to 3 dimensions
comp_ilr <- ilr(coDA_comp)
ilr_df <- as.data.frame(comp_ilr)
all_rescale <- bind_cols(CoDA_select, ilr_df)

#regression analysis #Mixed-effects model if repeated measures
model <-lmer(MH_Score ~ V1 + V2 + V3 + (1|Day), data = all_rescale)
summary(model)

#“Replacing 30 min sedentary with 30 min LPA increases outcome by X
# Use pivotCoord() or the coda.base package.

#--------------------------reassign minutes
#mean composition and helper to predict MH 
parts <- c("MVPA", "Sleep", "Sedentary", "LPA")

# mean composition (proportions)
mean_comp <- all_rescale %>%
  summarise(across(all_of(parts), mean, na.rm = TRUE)) %>%
  unlist()

mean_comp
# function: from a composition -> predicted MH (fixed effects only)
predict_MH <- function(comp_vec, model) {
  
  # guard against bad vectors
  if (any(is.na(comp_vec)) || any(comp_vec <= 0)) return(NA_real_)
  
  comp_vec <- comp_vec / sum(comp_vec)
  
  ilr_vals <- ilr(acomp(rbind(comp_vec)))
  newdata  <- data.frame(
    V1 = ilr_vals[,1],
    V2 = ilr_vals[,2],
    V3 = ilr_vals[,3]
  )
  as.numeric(predict(model, newdata = newdata, re.form = NA))
}

target <- "MVPA"
others <- setdiff(names(mean_comp), target)

# choose a safe delta range in minutes
safe_delta <- floor(min(mean_comp) * 1440)  # max we can move without going negative
delta_range <- seq(-safe_delta, safe_delta, by = 1)

# grid of (from, delta)
panel_MVPA <- expand.grid(
  from = others,
  delta_min = delta_range,
  KEEP.OUT.ATTRS = FALSE,
  stringsAsFactors = FALSE
)

# compute predictions
panel_MVPA$MH_pred <- mapply(function(from, dmin) {
  delta <- dmin / 1440
  
  comp_new <- mean_comp
  comp_new[target]    <- comp_new[target]    + delta
  comp_new[from]      <- comp_new[from]      - delta
  
  predict_MH(comp_new, model)
}, panel_MVPA$from, panel_MVPA$delta_min)

# drop impossible substitutions
panel_MVPA <- panel_MVPA[!is.na(panel_MVPA$MH_pred), ]

head(panel_MVPA)

#------------plot the panel
ggplot(panel_MVPA,
       aes(x = delta_min, y = MH_pred,
           linetype = from)) +
  geom_line(size = 0.8) +
  scale_linetype_manual(
    values = c(Sedentary = "dotted",
               LPA       = "twodash",
               Sleep     = "solid")
  ) +
  labs(x = "Change in time spent in MVPA (mins/day)",
       y = "Predicted mental health score",
       linetype = "Minutes taken from") +
  theme_bw()





