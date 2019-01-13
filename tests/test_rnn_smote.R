source("./func/rnn_smote.r")
source("./func/comp_dist_metric.r")

set.seed(1)
testfr <- data.frame(class=c(rep("maj", 15), rep("min", 5)),
                     numa=rnorm(20), numb=runif(20), numc=rf(20, 3, 1),
                     cata=as.factor(ifelse(runif(20)>0.5, "a", "b")),
                     catb=as.character(ifelse(runif(20)>0.5, "d", "e")),
                     stringsAsFactors=FALSE)

res1 <- rnn_smote_v1(data=pima, colname_target="Class", minority_label="positive", 
                     knn=3, multiply_min=1, 
                     use_n_cores=1, n_batches=NULL, 
                     dist_type=c("hvdm"),
                     seed_use=1)

res2 <- rnn_smote_v1(data=pima, colname_target="Class", minority_label="positive", 
                     knn=3, multiply_min=1, 
                     use_n_cores=2, n_batches=NULL, 
                     dist_type=c("heom"),
                     seed_use=1)

res3 <- rnn_smote_v1(data=pima, colname_target="Class", minority_label="positive", 
                     knn=3, multiply_min=1, 
                     use_n_cores=2, n_batches=8, 
                     dist_type=c("hvdm"),
                     seed_use=1)

res4 <- rnn_smote_v1(data=pima, colname_target="Class", minority_label="positive", 
                     knn=3, multiply_min=1, 
                     use_n_cores=4, n_batches=16, 
                     dist_type=c("hvdm"),
                     seed_use=1)
