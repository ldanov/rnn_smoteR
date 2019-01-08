# generate test data
set.seed(1)

testfr <- data.frame(class=c(rep("maj", 15), rep("min", 5)), 
            numa=rnorm(20), numb=runif(20), numc=rf(20, 3, 1),  
            cata=as.factor(ifelse(runif(20)>0.5, "a", "b")), 
            catb=as.character(ifelse(runif(20)>0.5, "d", "e")), 
            stringsAsFactors=FALSE)
