#################################
####### Safelevel - SMOTE #######
## data_origin - data frame containing only target and features and wihout NAs
## target - string of target column name
## k - number of nearest neighbours to evaluate on (odd number recommended)
## n_only_minority=TRUE Uses only minority neighbours for n evaluation
## n_only_minority=FALSE Uses minority and majority neighbours for n evaluation
## see line 3, Fig. 1, Bunkhumpornpat(2009)
## Returns a dataframe with only newly created observations
## String columns are returned as factors
## Lyubomir Danov, 2018
#################################

sl_smote <- function(data_origin, target, k, multiply_min, n_only_minority=TRUE, use_n_cores) {
  require(dplyr)
  # Currently does not handle any NAs
  if(anyNA(data_origin)) {
    stop("data_origin contains NA values")
  }
  
  use_n_cores <- ifelse(missing(use_n_cores), 1, use_n_cores)
  
  if(use_n_cores>1){
    require(parallel)
  }
  
  .sl_smote_cl_fun <- function(X, settings_supplier, lb_df, data) {
    lb_df_local <- lb_df[[X]] 
    list2env(settings_supplier, envir = environment())
    
    for (z in 1:nrow(lb_df_local)) {
      i <- lb_df_local$my_row_id[z]
      set.seed(i)
      
      
      data_sc <- scale(x=data, 
                       center = unname(unlist(data[i, ])), 
                       scale = c(apply(data, 2, max) - apply(data, 2, min)))
      
      # Evaluate categorical on match      
      if (length(categorical)>0) {
        for (cat in categorical) {
          data_sc[,cat] <- data_sc[,cat] != 0
        }
      }
      
      all_NNs <- (drop(data_sc^2 %*% rep(1, ncol(data_sc))))
      # always calculate # of minority on all neighbours
      # number of minorities within knn is sum(target_vars[kNNs])
      eval_kNNs <- order(all_NNs)[2:(k+1)]
      sl_p <- sum(target_vars[eval_kNNs])
      
      if(n_only_minority) {
        kNNs <- c()
        all_NNs[target_vars==0] <- NA
        kNNs <- order(all_NNs, na.last = TRUE)[2:(k+1)]
      } else {
        kNNs <- eval_kNNs
      }
      for (m_n in 1:multiply_min) {
        neig <- sample(k, 1)
        data_sc_n <- scale(x=data,
                           center = unname(unlist(data[kNNs[neig], ])),
                           scale = c(apply(data, 2, max) - apply(data, 2, min)))
        
        # Evaluate categorical on match      
        if (length(categorical)>0) {
          for (cat in categorical) {
            data_sc_n[,cat] <- data_sc_n[,cat] != 0
          }
        }
        
        # the n neighbour always needs all its' neighbours (majority and minority)
        kNNs_n <- order(drop(data_sc_n^2 %*% rep(1, ncol(data_sc_n))))[2:(k+1)]
        # print(paste0("i: ", i, "; knns: ", toString(kNNs_n)))
        sl_n <- sum(target_vars[kNNs_n])
        
        diff_n_p <- (data[kNNs[neig],] - data[i,])
        
        # if sl_p==0 & sl_n==0 do not generate new observation
        # <> all NAs which is new's default values, are filtered out later
        if (!(sl_p==0 & sl_n==0)) {
          if (sl_p==0 & sl_n>0) {
            gap <- 1
          } else if (sl_p>0 & sl_n==0) {
            gap <- 0
          } else if (sl_p==sl_n & sl_n>0) {
            gap <- runif(ncol(data))
          } else if (sl_p>sl_n) {
            # rand[0, 1/sl_r] = rand[0, sl_n/sl_p]
            gap <- runif(ncol(data), min = 0, max = (sl_n/sl_p))
          } else if (sl_p<sl_n) {
            # rand[1-sl_r, 1] = rand[(sl_n-sl_p)/sl_n, 1]
            gap <- runif(ncol(data), min = ((sl_n-sl_p)/sl_n), max = 1)
          }
          
          tmp_obs <- data.frame(data[i,] + gap * diff_n_p)
          
          if(length(categorical)>0){
            for (cat in categorical)
              tmp_obs[1, cat] <- c(data[kNNs[neig], cat], data[i, cat])[1+sample(0:1, 1)]
          }
          
          if(!exists("new_obs", envir = environment())) {
            new_obs <- tmp_obs
          } else {
            new_obs <- bind_rows(new_obs, tmp_obs)    
          }
        }
      }
      
    }  
    return(new_obs)
  }
  # Find label of minority, create vector of minority locations
  target_label <- names(which.min(table(data_origin[,target])))
  target_origin <- drop(data_origin[,target])
  target_vars <- as.numeric(target_origin==target_label)
  
  data_origin <- data_origin %>%
    mutate(my_row_id=row_number())
  lb_df <- data_origin[data_origin[,target]==target_label, ] %>%
    select(my_row_id) %>%
    mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
    split(., .$local_id)
    
  # Reencode character and factor cols as numeric
  data <- data_origin[,!colnames(data_origin) %in% c(target, "my_row_id")]
  
  colclasses <- sapply(data, class)
  categorical <- names(colclasses)[colclasses %in% c("character", "factor")]
  if(length(categorical)==0) categorical <- NULL
  categorical <- c()
  for (cats in 1:ncol(data)) {
    if (class(data[,cats])=="character") data[,cats] <- as.factor(data[,cats])
    if (class(data[,cats])=="factor") { 
      categorical <- c(categorical, colnames(data)[cats])
      data[,cats] <- as.numeric(data[,cats])
    }
  }
  
  settings_supplier <- list(multiply_min=multiply_min,
                            k=k,
                            target_vars=target_vars,
                            n_only_minority=n_only_minority,
                            categorical=categorical)
  
  
  if(use_n_cores>1){
    
    sl_smote_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl = sl_smote_cl, envir = environment(), varlist = c("settings_supplier", "lb_df", "data"))
    parallel::clusterCall(cl = sl_smote_cl, function() library(dplyr))
    on.exit(stopCluster(sl_smote_cl))
    
    list_new_obs <- parLapplyLB(cl = sl_smote_cl, 
                                X = 1:use_n_cores, 
                                fun = .sl_smote_cl_fun,
                                settings_supplier=settings_supplier,
                                lb_df = lb_df,
                                data=data)

    
  } else {
    
    list_new_obs <- lapply(X = 1:use_n_cores, 
                           FUN = .sl_smote_cl_fun,
                           settings_supplier=settings_supplier,
                           lb_df = lb_df,
                           data=data)
    
  }
  
  df_new <- list_new_obs %>% bind_rows(.)

  ## Refactor categorical vars  
  if(exists("categorical")){
    for (cat in categorical)
      df_new[,cat] <- factor(df_new[,cat],levels=1:nlevels(data_origin[,cat]),labels=levels(data_origin[,cat]))
  }

  ## Refactor target
  if(class(data_origin[,target])=="factor") {
    df_new[,target] <- factor(rep(target_label,nrow(df_new)),levels=levels(data_origin[,target]))
  } else {
    df_new[,target] <- target_label
  }
  
  return(df_new)
}
#END
