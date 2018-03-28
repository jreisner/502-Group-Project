library(data.table)
library(tidyverse)
library(lubridate)
df <- fread("historical2.csv")
df <- df[, -c("attributed_time")]

df$date <- ymd(df$date)
df$day <- day(df$date)

df$hour <- hour(hms(df$time))
df$minute <- minute(hms(df$time))
df$second <- second(hms(df$time))
df <- df[, -c("date", "time")]

df <- df %>%
  group_by(ip) %>%
  mutate(ip_clicks = n())  %>%
  ungroup() %>%
  group_by(ip, hour) %>%
  mutate(ip_hour_clicks = n())  %>%
  ungroup() %>%
  group_by(hour) %>%
  mutate(hour_clicks = n())  %>%
  ungroup()

oneway_LLR <- function(data, var1) {
  data %>%
    group_by_(var1) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>%
    ungroup() %>%
    select(LLR) %>%
    return()
}
st <- Sys.time()
df$app_llr <- oneway_LLR(df, "app")$LLR
Sys.time() - st
# Time difference of 0.6360621 secs

twoway_LLR <- function(data, var1, var2) {
  data %>%
    group_by_(var1, var2) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    ungroup() %>%
    unite_("grp", c(var1, var2), sep = ".") %>%
    select(grp, LLR) %>%
    return()
}
st <- Sys.time()
df$app.os <- twoway_LLR(df, "app", "os")$grp
df$app.os_llr <- twoway_LLR(df, "app", "os")$LLR
Sys.time() - st
# Time difference of 7.670145 secs


threeway_LLR <- function(data, var1, var2, var3) {
  data %>%
    group_by_(var1, var2, var3) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    ungroup() %>%
    unite_("grp", c(var1, var2, var3), sep = ".") %>%
    select(grp, LLR) %>%
    return()
}
st <- Sys.time()
df$app.os.hour <- threeway_LLR(df, "app", "os", "hour")$grp
df$app.os.hour_llr <- threeway_LLR(df, "app", "os", "hour")$LLR
Sys.time() - st
# Time difference of 19.05023 secs

fourway_LLR <- function(data, var1, var2, var3, var4) {
  data %>%
    group_by_(var1, var2, var3, var4) %>%
    mutate(LLR = log(sum(is_attributed) + 1/2) - log(sum(is_attributed == 0) + 1/2)) %>% 
    ungroup() %>%
    unite_("grp", c(var1, var2, var3, var4), sep = ".") %>%
    select(grp, LLR) %>%
    return()
}
st <- Sys.time()
df$app.os.hour.device <- fourway_LLR(df, "app", "os", "hour", "device")$grp
df$app.os.hour.device_llr <- fourway_LLR(df, "app", "os", "hour", "device")$LLR
Sys.time() - st
# Time difference of 23.18406 secs


