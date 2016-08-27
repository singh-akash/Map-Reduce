library(plyr)
arg2 <- commandArgs(trailingOnly = TRUE) 
file_list2 <- list.files(path = arg2[1], pattern = "part-r*", full.names = TRUE)
for (file in file_list2) {
  if (!exists("carr_data")){
    carr_data <- read.csv(file, header=FALSE, sep=",")
  }
  if (exists("carr_data")){
    temp_dataset <-read.csv(file, header=FALSE, sep=",")
    names(temp_dataset) <- names(carr_data)
    carr_data <-rbind(carr_data, temp_dataset)
    rm(temp_dataset)
  }
}
names(carr_data) <- c("Carrier", "Year", "Week","MedianPrice")
carr_data$combo <- paste(carr_data$Year,carr_data$Week, sep = "-")
carr_data$Date <- as.Date(paste("1", carr_data$Week, carr_data$Year, sep = "-"), format = "%w-%W-%Y")
mods <- lm(MedianPrice~Date, data=carr_data)
p <- plot(carr_data$MedianPrice ~ carr_data$Date, ylab = "Median Ticket Prices", xlab = "Year", main = carr_data$Carrier[1])
abline(mods)