library(plyr)
arg <- commandArgs(trailingOnly = TRUE) 
file_list <- list.files(path = arg[1], pattern = "part-r*", full.names = TRUE)
for (file in file_list) {
  if (!exists("d")){
    d <- read.csv(file, header=FALSE, sep=",")
  }
  if (exists("d")){
    temp_dataset <-read.csv(file, header=FALSE, sep=",")
    names(temp_dataset) <- names(d)
    d <-rbind(d, temp_dataset)
    rm(temp_dataset)
  }
}
names(d) <- c("Carrier", "Year", "ElaspedTime","AvgTicketPrice")
mods <- ddply(.data = d, .(Carrier,Year), function(df) coefficients(lm(AvgTicketPrice~ElaspedTime, data=df)))
write.csv(x = mods, file="./LM.csv", row.names = FALSE)

