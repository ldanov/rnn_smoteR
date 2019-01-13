#################################
####### Safelevel - SMOTE #######
## data - training dataset (with SMOTE already applied)
## colname_target - target variable
## n - splits in each iteration
## k - stopping_rounds where rejected obs<=p
## p - percentage from data to use as stopping_metric
## Defaults for n, k, p taken from original algorithm paper
## For details see Sáez, J. A., Luengo, J., Stefanowski, J., & Herrera, F. (2015). 
## SMOTE-IPF: Addressing the noisy and borderline examples problem in imbalanced 
## classification by a re-sampling method with filtering. Information Sciences, 
## 291(C), 184–203. https://doi.org/10.1016/j.ins.2014.08.051
## Lyubomir Danov, 2018
#################################

smote_ipf <- function(data, colname_target, n=9, k=3, p=0.01, silent=TRUE) {
  try_rweka <- require(RWeka)
  try_dplyr <- require(dplyr)
  missing_lib <- toString(deparse(substitute(try_rweka)), deparse(substitute(try_dplyr)))
  
  if(!try_rweka | !try_dplyr) {
    stop(paste0(missing_lib," not installed, but necessary!"))
  }
  
  # Currently does not handle any NAs
  if(anyNA(data)) {
    stop("data contains NA values")
  }
  
  if(missing(data) | missing(colname_target)) {
    stop("missing data or colname_target")
  }
  
  colclasses <- sapply(data, class)
  categorical <- names(colclasses)[colclasses %in% c("character", "factor")]
  char2fac <- names(colclasses)[colclasses %in% c("character")]
  for (conv in char2fac) {
    if (class(data[,conv])=="character") data[,conv] <- as.factor(data[,conv])
  }
  
  # initialise
  data$row_id <- 1:nrow(data)
  colnames(data)[colnames(data)==colname_target] <- "smote_ipf_target"
  k_i <- 1
  eval_k_i <- TRUE
  
  # create model formula
  features <- colnames(data)[colnames(data)!=colname_target]
  full_formula <- as.formula(paste0("smote_ipf_target", " ~ ", paste(features, collapse = " + ")))
  
  while(eval_k_i) {
    # first rearrange data via sample
    # then rep_len(c(1:n), length.out = n())
    set.seed(k_i)
    data <- data[sample(x = 1:nrow(data), replace = FALSE), ]
    data$split_k_i <- rep_len(c(1:n), length.out = nrow(data))
    
    for (n_i in 1:n) {
      E_n_i <- data[data$split_k_i==n_i,]
      c45_n_i <- J48(formula=full_formula, data=E_n_i, 
                     control = Weka_control())
      
      predict_n_i <- data %>%
        select(one_of("smote_ipf_target", "row_id")) %>% 
        mutate(model_n_i=n_i) %>%
        mutate(label=predict(c45_n_i, newdata=data, type="class")) 
      
      if(n_i==1) {
        predict_k_i <- predict_n_i
      } else {
        predict_k_i <- bind_rows(predict_k_i, predict_n_i)
      }
      
      rm(E_n_i, c45_n_i, predict_n_i)
    }
    
    cast_vote_k_i <- predict_k_i %>%
      mutate(eval_predict=smote_ipf_target==label) %>%
      group_by(row_id) %>%
      summarise(votes=sum(eval_predict)) %>%
      filter(votes<(n/2))
    
    p_k_i <- nrow(cast_vote_k_i)/nrow(data)
    data <- data %>% 
      anti_join(cast_vote_k_i, by="row_id")
    
    if(k_i==1) {
      eval_df <-  data_frame(k_i=1, percent_removed=p_k_i) %>%
        mutate(eval_stop=percent_removed<p)
    } else {
      eval_df <- eval_df %>%
        bind_rows(data_frame(k_i=k_i, percent_removed=p_k_i)) %>%
        mutate(eval_stop=percent_removed<p)
    }
    if(!silent){
      print(paste0(Sys.time(), " iteration ", k_i, " result: "))
      print(as.data.frame(eval_df[k_i,]))
    }
    
    eval_k_i <- sum(eval_df$eval_stop[max(c((k_i-2), 1)):k_i])!=k
    k_i <- k_i + 1
    rm(p_k_i)
  }
  
  colnames(data)[colnames(data)=="smote_ipf_target"] <- colname_target
  return(data)
}

#END