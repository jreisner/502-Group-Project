library(data.table)
library(tidyverse)
library(lubridate)
library(caret)
library(ranger)
library(ROSE)
library(parallel)
n.core <- detectCores()
# ------------------------------------------------------------------------------
oneway_LLR <- function(data, var1) {
  data %>%
    group_by_(var1) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>%
    ungroup() %>%
    select(LLR) %>%
    return()
}

twoway_LLR <- function(data, var1, var2) {
  data %>%
    group_by_(var1, var2) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    ungroup() %>%
    unite_("grp", c(var1, var2), sep = ".") %>%
    select(grp, LLR) %>%
    return()
}

oneway_LLR_summ <- function(data, var1) {
  data %>%
    group_by_(var1) %>%
    summarise(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    return()
}

twoway_LLR_summ <- function(data, var1, var2) {
  data %>%
    group_by_(var1, var2) %>%
    summarise(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    ungroup() %>%
    unite_("grp", c(var1, var2), sep = ".") %>%
    select(grp, LLR) %>%
    return()
}

twoway_name_func <- function(data, var1, var2) {
  data %>%
    unite_("grp", c(var1, var2), sep = ".") %>%
    select(grp) %>%
    return()
}
# ------------------------------------------------------------------------------

df <- fread("train.csv")
df <- df[, -c("attributed_time")]
df <- separate(df, click_time, sep = "\\s", into = c("date", "time"), 
               remove = TRUE)

df$date <- ymd(df$date)
df$day <- day(df$date)

df$hour <- hour(hms(df$time))
df$minute <- minute(hms(df$time))
df$second <- second(hms(df$time))
df <- df[, -c("date", "time")]

nr <- nrow(df)

set.seed(1)
hist_ind <- sample(1:nr, 0.5 * nr)
hist_df <- df[hist_ind,]

tr_df <- df[-hist_ind,]

test_df <- fread("test.csv")
test_df <- separate(test_df, click_time, sep = "\\s", into = c("date", "time"), 
                    remove = TRUE)

test_df$date <- ymd(test_df$date)
test_df$day <- day(test_df$date)

test_df$hour <- hour(hms(test_df$time))
test_df$minute <- minute(hms(test_df$time))
test_df$second <- second(hms(test_df$time))
test_df <- test_df[, -c("date", "time")]

# FEATURE ENGINEERING ON hist_df -----------------------------------------------
# one-way LLRs ---
cat_vars <- c(2, 3, 4, 5, 8)
hist_df$app_llr <- oneway_LLR(hist_df, "app")
hist_df$dev_llr <- oneway_LLR(hist_df, "device")
hist_df$os_llr <- oneway_LLR(hist_df, "os")
hist_df$channel_llr <- oneway_LLR(hist_df, "channel")
hist_df$hour_llr <- oneway_LLR(hist_df, "hour")

oneway_grp_names <- c("app", "device", "os", "channel", "hour")
oneway_llr_names <- c("app_llr", "dev_llr", "os_llr", "channel_llr", "hour_llr")


# two-way LLRs ---
two_combns <- t(combn(cat_vars, 2))

twoway_grp_names <- unlist(lapply(1:nrow(two_combns), function(x) {
  paste(names(hist_df)[two_combns[x, 1]], 
        names(hist_df)[two_combns[x, 2]], sep = ".")
}))

twoway_llr_names <- unlist(lapply(1:nrow(two_combns), function(x) {
  paste0(names(hist_df)[two_combns[x, 1]], ".",
         names(hist_df)[two_combns[x, 2]], "_llr")
}))


twoway_grp_cols <- as.data.frame(do.call(cbind, lapply(1:nrow(two_combns), function(x) twoway_LLR(hist_df, names(hist_df)[two_combns[x, 1]], 
                                                                                                  names(hist_df)[two_combns[x, 2]])$grp)))
names(twoway_grp_cols) <- twoway_grp_names

twoway_llr_cols <- as.data.frame(do.call(cbind, lapply(1:nrow(two_combns), function(x) twoway_LLR(hist_df, names(hist_df)[two_combns[x, 1]], 
                                                                                                  names(hist_df)[two_combns[x, 2]])$LLR)))
names(twoway_llr_cols) <- twoway_llr_names

hist_df <- cbind(hist_df, twoway_grp_cols, twoway_llr_cols)


# ADDING FEATURES FROM hist_df TO tr_df ----------------------------------------
# one-ways
# Make a list of tbls that give unique grp-LLR pairs to easily join with train/test data
oneway_summ_tbls <- lapply(1:length(cat_vars), function(x) {
  oneway_LLR_summ(hist_df, oneway_grp_names[x])
})

oneway_summ_tbls <- lapply(1:length(cat_vars), function(x) {
  setNames(oneway_summ_tbls[[x]], nm = c(oneway_grp_names[x], 
                                         oneway_llr_names[x]))
})
names(oneway_summ_tbls) <- oneway_grp_names

# add LLR values to tr_df
tr_df <- tr_df %>%
  left_join(oneway_summ_tbls[[1]]) %>%
  left_join(oneway_summ_tbls[[2]]) %>%
  left_join(oneway_summ_tbls[[3]]) %>%
  left_join(oneway_summ_tbls[[4]]) %>%
  left_join(oneway_summ_tbls[[5]])


# two-ways ----
# Make a list of tbls that give unique grp-LLR pairs to easily join with train/test data
twoway_summ_tbls <- lapply(1:nrow(two_combns), function(x) {
  twoway_LLR_summ(hist_df, names(hist_df)[two_combns[x, 1]], names(hist_df)[two_combns[x, 2]])
})

# rename tbls and columns of tbls in the list
twoway_summ_tbls <- lapply(1:10, function(x) {
  setNames(twoway_summ_tbls[[x]], nm = c(twoway_grp_names[x], twoway_llr_names[x]))
})
names(twoway_summ_tbls) <- twoway_grp_names


# make columns that give the two-way groups
tr_df.twoway_grps <- as.data.frame(do.call(cbind, lapply(1:nrow(two_combns), function(x) twoway_LLR(tr_df, names(tr_df)[two_combns[x, 1]], 
                                                                                                    names(tr_df)[two_combns[x, 2]])$grp)))
# name such columns
names(tr_df.twoway_grps) <- twoway_grp_names

# append tr_df
tr_df <- cbind.data.frame(tr_df, tr_df.twoway_grps)

# add LLR values to tr_df
tr_df <- tr_df %>%
  left_join(twoway_summ_tbls[[1]]) %>%
  left_join(twoway_summ_tbls[[2]]) %>%
  left_join(twoway_summ_tbls[[3]]) %>%
  left_join(twoway_summ_tbls[[4]]) %>%
  left_join(twoway_summ_tbls[[5]]) %>%
  left_join(twoway_summ_tbls[[6]]) %>%
  left_join(twoway_summ_tbls[[7]]) %>%
  left_join(twoway_summ_tbls[[8]]) %>%
  left_join(twoway_summ_tbls[[9]]) %>%
  left_join(twoway_summ_tbls[[10]])

# what to do with missing values?
#   for now I'll set them equal to zero.
tr_df[is.na(tr_df)] <- 0

# ADDING FEATURES FROM hist_df TO test_df ----------------------------------------
# one-ways
# Make a list of tbls that give unique grp-LLR pairs to easily join with train/test data
# add LLR values to tr_df
test_df <- test_df %>%
  left_join(oneway_summ_tbls[[1]]) %>%
  left_join(oneway_summ_tbls[[2]]) %>%
  left_join(oneway_summ_tbls[[3]]) %>%
  left_join(oneway_summ_tbls[[4]]) %>%
  left_join(oneway_summ_tbls[[5]])


# two-ways ----
# make columns that give the two-way groups
test_cat_vars <- c(3, 4, 5, 6, 8)
test_two_combns <- t(combn(test_cat_vars, 2))

test_df.twoway_grps <- 
  data.frame(do.call(cbind, lapply(1:nrow(test_two_combns), function(x) {
    twoway_name_func(test_df, names(test_df)[test_two_combns[x, 1]],
                     names(test_df)[test_two_combns[x, 2]])
  })))


# name such columns
names(test_df.twoway_grps) <- twoway_grp_names

# append test_df
test_df <- cbind.data.frame(test_df, test_df.twoway_grps)

# add LLR values to test_df
test_df <- test_df %>%
  left_join(twoway_summ_tbls[[1]]) %>%
  left_join(twoway_summ_tbls[[2]]) %>%
  left_join(twoway_summ_tbls[[3]]) %>%
  left_join(twoway_summ_tbls[[4]]) %>%
  left_join(twoway_summ_tbls[[5]]) %>%
  left_join(twoway_summ_tbls[[6]]) %>%
  left_join(twoway_summ_tbls[[7]]) %>%
  left_join(twoway_summ_tbls[[8]]) %>%
  left_join(twoway_summ_tbls[[9]]) %>%
  left_join(twoway_summ_tbls[[10]])

# what to do with missing values?
#   for now I'll set them equal to zero.
test_df[is.na(test_df)] <- 0

# ------------------------------------------------------------------------------
regressors <- names(tr_df)[c(11:15, 26:35)]
form <- as.formula(paste("is_attributed", paste(regressors, collapse = " + "), sep = " ~ "))

tr_bal_under <- ovun.sample(form, data = tr_df,
                            method = "under", N = 2 * sum(tr_df$is_attributed), seed = 29)

tr_bal_under$data$is_attributed %>% table()
balanced_tr_df <- tr_bal_under$data
balanced_tr_df$is_attributed <- as.factor(balanced_tr_df$is_attributed)
# ------------------------------------------------------------------------------
balanced_tr_df$fraud <- as.factor(ifelse(balanced_tr_df$is_attributed == 1, "Y", "N"))

tr_control <- trainControl(method = "cv", number = 10, classProbs = TRUE,
                           summaryFunction = twoClassSummary)
rfTune <- train(x = balanced_tr_df[, 2:16], y = balanced_tr_df[, 17],
                tuneGrid = data.frame(mtry = c(1, 1 + 2 * (1:7)),
                                      splitrule = "gini",
                                      min.node.size = 1),
                num.threads = n.core,
                method = "ranger", 
                trControl = tr_control)
rfTune
saveRDS(rfTune, "03292018_rangerRF.rds")

rf_test_pred <- predict(rfTune, test_df)
saveRDS(rf_test_pred, "03292018 RF test preds.rds")