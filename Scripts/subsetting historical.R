library(data.table)
library(tidyr)
df <- fread("~/Documents/502 Group Project/train.csv",
            nrows = 5000000, skip = 50)

# rows of train.csv that make of df
ind <- 1 + 50 * (1:5000000)

# column names
names(df) <- c("ip", "app", "device", "os", "channel", "click_time", "attributed_time",
                "is_attributed")
head(df)

# only 0.18% of observations are fradulent
sum(df$is_attributed) / nrow(df) * 100

# separating date and time. lubridate package can be used to further decompose
df <- separate(df, click_time, sep = "\\s", into = c("date", "time"), remove = FALSE)

# save csv
fwrite(df, "~/Documents/502 Group Project/historical.csv")


