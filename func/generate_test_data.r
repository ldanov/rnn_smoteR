# generate test data

source("~/projects/rnn_smoteR/func/heom_dist.r")
source("~/projects/rnn_smoteR/func/rnn_smote.r")
set.seed(1)

testfr <- data.frame(class=c(rep("maj", 10), rep("min", 2)), 
            numa=rnorm(12), numb=runif(12), numc=rf(12, 3, 1),  
            cata=as.factor(ifelse(runif(12)>0.5, "a", "b")), 
            catb=as.character(ifelse(runif(12)>0.5, "d", "e")), 
            stringsAsFactors=FALSE)

rnn_smote_v1(data=testfr, knn=3, colname_target="class", minority_label="min", use_n_cores=2, multiply_min=2)