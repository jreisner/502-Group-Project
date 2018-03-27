library(data.table)
library(tidyr)
library(dplyr)
# original data file has 184,903,890 rows
skip_ind <- c(1, (1:7) * 23112986 + 1)
skip_ind
nr <- 23112986

read_fun <- function(i) {
  df <- fread("~/Documents/502 Group Project/train.csv", header = FALSE, 
              skip = skip_ind[i],
              nrows = nr)
  set.seed(i)
  df_ind <- sample(1:nr, 625000)
  df <- df[df_ind,]
}
df1 <- read_fun(1)
df2 <- read_fun(2)
df3 <- read_fun(3)
df4 <- read_fun(4)
df5 <- read_fun(5)
df6 <- read_fun(6)
df7 <- read_fun(7)
df8 <- read_fun(8)

df <- bind_rows(df1, df2, df3, df4, df5, df6, df7, df8)
names(df) <- c("ip", "app", "device", "os", "channel", "click_time", 
               "attributed_time", "is_attributed")

# only 0.245% of observations are fradulent
sum(df$is_attributed) / nrow(df) * 100

# separating date and time. lubridate package can be used to further decompose
df <- separate(df, click_time, sep = "\\s", into = c("date", "time"), remove = FALSE)

# save as a csv
fwrite(df, "historical2.csv")




