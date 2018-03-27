library(data.table)
library(tidyr)
library(dplyr)

df2 <- fread("~/Documents/502 Group Project/train.csv", header = TRUE, nrows = 20000000)
fwrite(df2, "~/Documents/502 Group Project/df2.csv")
df3 <- fread("~/Documents/502 Group Project/train.csv", header = FALSE, nrows = 20000000, skip = 30000000)
fwrite(df3, "~/Documents/502 Group Project/df3.csv")
df4 <- fread("~/Documents/502 Group Project/train.csv", header = FALSE, nrows = 20000000, skip = 70000000)
fwrite(df4, "~/Documents/502 Group Project/df4.csv")
df5 <- fread("~/Documents/502 Group Project/train.csv", header = FALSE, nrows = 20000000, skip = 110000000)
fwrite(df5, "~/Documents/502 Group Project/df5.csv")
df6 <- fread("~/Documents/502 Group Project/train.csv", header = FALSE, nrows = 20000000, skip = 150000000)
fwrite(df6, "~/Documents/502 Group Project/df6.csv")


names(df2) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
               "is_attributed")
set.seed(2)
df2_ind <- sample(1:nrow(df2), 1000000)
df2_sub <- df2[df2_ind,]
rm(df2)

names(df3) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
               "is_attributed")
set.seed(3)
df3_ind <- sample(1:nrow(df3), 1000000)
df3_sub <- df3[df3_ind,]
rm(df3)

names(df4) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
               "is_attributed")
set.seed(4)
df4_ind <- sample(1:nrow(df4), 1000000)
df4_sub <- df4[df4_ind,]
rm(df4)

names(df5) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
                "is_attributed")
set.seed(5)
df5_ind <- sample(1:nrow(df5), 1000000)
df5_sub <- df5[df5_ind,]
rm(df5)

names(df6) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
                "is_attributed")
set.seed(6)
df6_ind <- sample(1:nrow(df6), 1000000)
df6_sub <- df6[df6_ind,]
rm(df6)

# combine into one data frame
df <- bind_rows(df2_sub, df3_sub, df4_sub, df5_sub, df6_sub)

# clear environment of unnecessary objects
rm(df2_sub)
rm(df3_sub)
rm(df4_sub)
rm(df5_sub)
rm(df6_sub)

# only 0.26% of observations are fradulent
sum(df$is_attributed) / nrow(df) * 100

# separating date and time. lubridate package can be used to further decompose
df <- separate(df, click_time, sep = "\\s", into = c("date", "time"), remove = FALSE)

# save as a csv
fwrite(df, "historical.csv")



