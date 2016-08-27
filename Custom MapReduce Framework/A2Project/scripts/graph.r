#Author: Christopher Willig

library(ggplot2)
library(reshape2)

args <- commandArgs(trailingOnly=TRUE)
mydata = read.csv(args[1])
active2015 <- subset(mydata, Active2015 == 1)
sortedOnFlightCount <- active2015[with(active2015, order(-TotalFlights)),]
top10 <- head(sortedOnFlightCount, 10)
#remove active2015 column
top10$Active2015 <- NULL
#remove TotalFlights (don't need when plotting mean monthly ticket prices for airlines)
top10$TotalFlights <- NULL

#Go through each row in the frame, for each column in that row, create a graph (where the first value encountered is the airline)
row.names(top10) <- 1:nrow(top10) #Renumber rows.

df <- melt(top10)
linePlot = ggplot(df, aes(variable, value, group=factor(Airline))) + geom_line(aes(color=factor(Airline))) + labs(title="Airline Monthly Mean Ticket Prices", x="Month", y="Mean Ticket Price (Dollars)") + geom_point() + scale_y_continuous(breaks=round(seq(min(df$value), max(df$value), by=25), 1))
ggsave(filename="plot.png", plot=linePlot, width=20, height=30, dpi=120)
