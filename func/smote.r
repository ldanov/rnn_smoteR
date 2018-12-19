#################################
####### SMOTE ###################
## data_origin - data frame containing only target and features and wihout NAs
## target - string of target column name
## target_label - which label is the minority for which synth obs are generated
## multiply_min - how many synthetic observations to generate for each existing minority obs
## use_n_cores - should synth obs generation be parallelized; on how many cores (DEFAULT:1)
## handle_categorical - how should factor or character columns be returned. Currently only returns character columns.
## k - number of nearest neighbours to evaluate on (odd number recommended)
## Chawla et al. (2002)
## Returns a dataframe with only newly created observations
## String columns are returned as factors
## Lyubomir Danov, 2018
#################################

smote <- function(data_origin, target, target_label, k, multiply_min, use_n_cores, handle_categorical=c("character")) {
  require(dplyr)
  # Currently does not handle any NAs
  if(anyNA(data_origin)) {
    stop("data_origin contains NA values")
  }
  if("data.table" %in% class(data_origin)) {
    stop("data_origin cannot be of class data.table. Convert to data.frame and try again.")
  }
  
  use_n_cores <- ifelse(missing(use_n_cores), 1, use_n_cores)
  
  if(use_n_cores>1){
    require(parallel)
  }
  
  .smote_cl_fun <- function(X, settings_supplier, lb_df, data_fr) {
    lb_df_local <- lb_df[[X]] 
    list2env(settings_supplier, envir = environment())
    
    for (z in 1:nrow(lb_df_local)) {
      i <- lb_df_local$my_row_id[z]
      set.seed(i)
      
      data_fr_sc <- scale(x=data_fr, 
                          center = unname(unlist(data_fr[i, ])), 
                          scale = c(apply(data_fr, 2, max) - apply(data_fr, 2, min)))
      
      # Evaluate categorical on match      
      
      if (length(categorical)>0) {
        for (cat in categorical) {
          data_fr_sc[,cat] <- data_fr_sc[,cat] != 0
        }
      }
      
      
      all_NNs <- (drop(data_fr_sc^2 %*% rep(1, ncol(data_fr_sc))))
      # always calculate # of minority on all neighbours
      # number of minorities within knn is sum(target_vars[kNNs])
      kNNs <- order(all_NNs)[2:(k+1)]
      
      for (m_n in 1:multiply_min) {
        neig <- sample(k, 1)
        
        diff_n_p <- (data_fr[kNNs[neig],] - data_fr[i,])
        gap <- runif(ncol(data_fr))
        
        tmp_obs <- data.frame(data_fr[i,] + gap * diff_n_p)
        
        # if(anyNA(tmp_obs) ) {
        #   print(paste0("NAs introduced in i=",i,"; m_n=", m_n,"."))
        # }
        
        if(length(categorical)>0){
          for (cat in categorical)
            tmp_obs[1, cat] <- c(data_fr[kNNs[neig], cat], data_fr[i, cat])[1+sample(0:1, 1)]
        }
        
        if(!exists("new_obs", envir = environment())) {
          new_obs <- tmp_obs
        } else {
          new_obs <- bind_rows(new_obs, tmp_obs)    
        }
      }
    }
    return(new_obs)    
  }  

  # filter only minority class, remove class label column  
  data_fr <- data_origin[data_origin[,target]==target_label, ]
  data_fr <- data_fr[,!colnames(data_fr) %in% c(target)]
  lb_df <- data_fr %>%
    transmute(my_row_id=row_number()) %>%
    mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
    split(., .$local_id)
  
  # Reencode character and factor cols as numeric
  class_cat_col <- sapply(data_fr, class)
  categorical_cols <- class_cat_col[class_cat_col %in% c("character", "factor")]
  
  categorical_map <- list()
  if(length(categorical_cols)==0) categorical_cols <- NULL

  for (cats in names(categorical_cols)) {
    if (categorical_cols[cats]=="character") {
        data_fr[,cats] <- as.factor(data_fr[,cats])    
    }
    if (categorical_cols[cats]=="factor") { 
      categorical_map[[cats]] <- data.frame(fac_val=unique(data_origin[,cats])) %>%
                     mutate(num_val=as.numeric(fac_val),
                            chr_val=as.character(fac_val))
      data_fr[,cats] <- as.numeric(data_fr[,cats])
    }
  }
  
  settings_supplier <- list(multiply_min=multiply_min,
                            k=k,
                            categorical=names(categorical_cols))
  
  if(use_n_cores>1){
    
    smote_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl = smote_cl, envir = environment(), varlist = c("settings_supplier", "lb_df", "data_fr"))
    parallel::clusterCall(cl = smote_cl, function() library(dplyr))
    on.exit(stopCluster(smote_cl))
    
    list_new_obs <- parLapplyLB(cl = smote_cl,
                                X = 1:use_n_cores,
                                fun = .smote_cl_fun,
                                settings_supplier=settings_supplier,
                                lb_df = lb_df,
                                data_fr=data_fr)
    
    
  } else {
    
    list_new_obs <- lapply(X = 1:use_n_cores,
                           FUN = .smote_cl_fun,
                           settings_supplier=settings_supplier,
                           lb_df = lb_df,
                           data_fr=data_fr)
    
  }
  
  df_new <- list_new_obs %>% 
    bind_rows(.) %>%
    mutate(!!sym(target):=target_label) 

  for (cat in names(categorical_cols)) {
        df_new[,cat] <- data.frame(pre=unname(unlist(df_new[,cat]))) %>%
            left_join(categorical_map[[cat]], by=c("pre"="num_val")) %>%
            pull(chr_val)
  }

  if(handle_categorical!="character") {

  } else {
      df_new <- df_new %>%
        bind_rows(data_origin)
  }

  return(df_new)
  # return(list(settings_supplier, data_fr, lb_df))
}
#END