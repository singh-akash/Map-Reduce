library(ggplot2)
library(grid)
library(rmarkdown)
data <- read.csv("results.csv", header = FALSE, sep = ",")
names(data) <- c("Task", "Environment", "Time")
axis.tasks = function(tasks) {
  structure(
    list(groups=tasks),
    class = c("element_custom","element_blank"), 
    text = element_text(size = 10, vjust = 1, angle = 90)
  )
}
element_grob.element_custom <- function(element, x,...)  {
  cat <- list(...)[[1]]
  tasks <- element$task
  ll <- by(data$Task,data$Environment,I)
  tt <- as.numeric(x)
  grbs <- Map(function(z,t){
    labs <- ll[[z]]
    vp = viewport(
      x = unit(t,'native'), 
      height=unit(2,'line'),
      width=unit(diff(tt)[1],'native'),
      xscale=c(0,length(labs)))
    grid.rect(vp=vp)
    textGrob("",x= unit(seq_along(labs)-0.5,
                        'native'),
             y=unit(2,'line'),
             vp=vp,
             vjust = 1,
             hjust = 0)
  },cat,tt)
  g.X <- textGrob(cat, x=x)
  gTree(children=gList(do.call(gList,grbs),g.X), cl = "custom_axis")
}
grobHeight.custom_axis = 
  heightDetails.custom_axis = function(x, ...)
    unit(3, "lines")
p <- ggplot(data=data, aes(x=Environment, y=Time, fill=Task), xlab("Environment")) + 
  geom_bar(position = position_dodge(width=0.9),stat='identity') +
  geom_text(aes(label=paste(Time)),
            position=position_dodge(width=0.9), vjust=-0.25)+
  theme(axis.text.x = axis.tasks(unique(data$Task)))
p + ggtitle("Performance Evaluation")
render("Assignment_3_Report.Rmd","pdf_document")