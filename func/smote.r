#################################
####### SMOTE ###################
## data - data frame containing only target column and features and wihout NAs
## colname_target - string of target column name
## minority_label - label of the minority class for which synth obs are generated
## multiply_min - how many synthetic observations to generate for each existing minority obs
## use_n_cores - should synth obs generation be parallelized; on how many cores (DEFAULT:1)
## n_batches - in how many batches to split the observations (DEFAULT: number of minority obs)
## handle_categorical - how should factor or character columns be returned. Currently only returns character columns.
## k - number of nearest neighbours to evaluate on (odd number recommended)
## Chawla et al. (2002)
## Returns a dataframe with only newly created observations
## Categorical columns are returned as strings
## Lyubomir Danov, 2018
#################################

smote <- function(data, colname_target, minority_label, 
                  knn, multiply_min, use_n_cores=1, n_batches=NULL,
                  handle_categorical=c("character"), dist_type=c("hvdm", "heom"),
                  seed_use=NA) {
  require(dplyr)
  # Currently does not handle any NAs
  if(anyNA(data)) {
    stop("data contains NA values")
  }

  if("data.table" %in% class(data)) {
    stop("data cannot be of class data.table. Convert to data.frame and try again.")
  }

  if (missing(colname_target)) {
    stop("colname_target arg not specified")
  } else if (!is.character(colname_target) | length(colname_target)!=1) {
    stop("colname_target arg must be a single string") 
  } else if (!colname_target %in% colnames(data)) {
    stop("colname_target arg must be the name of a column in data") 
  } else if("class_col" %in% colnames(data) & colname_target!="class_col") {
    stop("only colname_target can be named class_col")
  }

  for (k in c("key_id")) {
    if(k %in% colnames(data)) {
      stop(paste0("data cannot contain column named ", k))
    }
  }

  if(length(dist_type)!=1){
      warning("more than one distance metric selected; using default hvmd")
      dist_type="hvdm"
  }
    
  if(is.na(seed_use)) {
      seed_use <- as.integer(Sys.time())
      warning(paste0("seed_use not set; automatically set to current unix time, ", seed_use))
  }

  if(use_n_cores>1){
    require(parallel)
  }
  
  n_min <- sum(data[,colname_target]==minority_label)
  if(is.null(n_batches)) {
      warning("n_batches missing. assigning default value of n minorities")
      n_batches <- n_min
  } 
  
  if (use_n_cores>n_min) {
      warning("use_n_cores larger than number of minority observations, currently not supported. 
                Overwritting use_n_cores and n_batches with n_minority for smote calculation.")
      use_n_cores <- n_min
  } else if(use_n_cores>n_batches) {
    warning("n_batches smaller than assigned cores; n_batches overwritten to number of cores")
      n_batches <- use_n_cores
  } else if(n_batches>n_min) {
      warning("n_batches bigger than number of minorities; n_batches overwritten to number of minorities")
      n_batches <- n_min
  }
  
  data_fr <- data %>%
    mutate(key_id=row_number()) %>%
    rename(class_col = !!sym(colname_target)) %>%
    select(key_id, class_col, everything()) %>%
    mutate_if(is.factor, as.character)
  
  dist_min <- comp_dist_metric(dplyr::filter(data_fr, class_col==minority_label), 
                             colname_target="class_col", 
                             use_n_cores=use_n_cores, 
                             n_batches=use_n_cores, 
                             dist_type=dist_type) %>%
    dplyr::filter(key_id_x!=key_id_y) %>%
    group_by(key_id_x) %>%
    top_n(n=-knn, wt=dist) %>%
    rename(neigh=key_id_y, key_id=key_id_x) %>%
    select(key_id, neigh) %>%
    ungroup()

  lb_df <- dist_min %>%
    distinct(key_id) %>%
    mutate(local_id=sort(rep_len(c(1:n_batches), length.out = n()))) %>%
    left_join(dist_min, by="key_id") %>%
    split(., .$local_id)

  col_classes <- sapply(data_fr, class)
  col_classes[names(col_classes) %in% c("class_col", "key_id")] <- NA
  categorical_cols <- col_classes[col_classes=="character" & !is.na(col_classes)]
  if(length(categorical_cols)==0) {
    data_fr <- data_fr %>%
      mutate(my_c_col_temp="a")
    categorical_cols <- c("my_c_col_temp"="character")
  }
  numerical_cols <- col_classes[col_classes!="character" & !is.na(col_classes)]
  if(length(numerical_cols)==0) {
    data_fr <- data_fr %>%
      mutate(my_n_col_temp=1)
    numerical_cols <- c("my_n_col_temp"="integer")
  }

  settings_supplier <- list(multiply_min=multiply_min,
                            seed=seed_use,
                            categorical=names(categorical_cols),
                            numerical=names(numerical_cols))
  
  if(use_n_cores>1){
    
    smote_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl = smote_cl, envir = environment(), 
        varlist = c("settings_supplier", "lb_df", "data_fr"))
    parallel::clusterCall(cl = smote_cl, lapply, c("dplyr", "tidyr"), 
                          function(x) library(x, character.only = TRUE))
    on.exit(stopCluster(smote_cl))
    
    list_new_obs <- parLapplyLB(cl = smote_cl,
                                X = 1:n_batches,
                                fun = .smote_cl_fun,
                                settings_supplier=settings_supplier,
                                lb_df = lb_df,
                                data_fr=data_fr)
    
  } else {
    
    list_new_obs <- lapply(X = 1:n_batches,
                           FUN = .smote_cl_fun,
                           settings_supplier=settings_supplier,
                           lb_df = lb_df,
                           data_fr=data_fr)
    
  }
  
  df_new <- list_new_obs %>% 
    bind_rows(.) %>%
    select(one_of(names(col_classes))) %>%
    mutate(!!sym(colname_target):=minority_label)  %>%
    as.data.frame(., row_names=NULL) 

  return(df_new)
  # return(list(settings_supplier, data_fr, lb_df))
}


.smote_cl_fun <- function(X, settings_supplier, lb_df, data_fr) {
  lb_df_local <- lb_df[[X]] 
  list2env(settings_supplier, envir = environment())
  
  df_cat_orig <- lb_df_local %>%
    distinct(key_id) %>%
    left_join(data_fr, by="key_id") %>%
    select(key_id, one_of(categorical)) %>%
    gather(feat_name, feat_value_cat, -key_id)
  
  df_num_orig <- lb_df_local %>%
    distinct(key_id) %>%
    left_join(data_fr, by="key_id") %>%
    select(key_id, one_of(numerical)) %>%
    gather(feat_name, feat_value_num, -key_id)
  
  for (mult in seq_len(multiply_min)) {
    set.seed(as.integer(mult+seed))
    lb_df_local_tmp <- lb_df_local %>%
      group_by(key_id) %>%
      sample_n(1) %>%
      ungroup()
    
    df_cat_new <- lb_df_local_tmp %>%
      left_join(data_fr %>% rename(neigh=key_id), by=c("neigh")) %>%
      select(key_id, one_of(categorical)) %>%
      gather(feat_name, feat_value_cat, -key_id) %>%
      full_join(df_cat_orig, 
                by=c("key_id", "feat_name"), 
                suffix=c("_n", "_o")) %>%
      mutate(gap=runif(n=n())) %>%
      mutate(feat_value_cat_synth=ifelse(gap>0.5, feat_value_cat_n, feat_value_cat_o)) %>%
      select(key_id, feat_name, feat_value_cat_synth) %>%
      spread(feat_name, feat_value_cat_synth)
    
    df_num_new <- lb_df_local_tmp %>%
      left_join(data_fr %>% rename(neigh=key_id), by=c("neigh")) %>%
      select(key_id, one_of(numerical)) %>%
      gather(feat_name, feat_value_num, -key_id) %>%
      full_join(df_num_orig, 
                by=c("key_id", "feat_name"), 
                suffix=c("_n", "_o")) %>%
      mutate(gap=runif(n())) %>%
      mutate(diff=feat_value_num_n - feat_value_num_o) %>%
      mutate(feat_value_num_synth=feat_value_num_o + (gap*diff)) %>%
      select(key_id, feat_name, feat_value_num_synth) %>%
      spread(feat_name, feat_value_num_synth)
    
    df_new_tmp <- df_cat_new %>%
      full_join(df_num_new, by="key_id")
    
    if (mult==1) {
      new_obs <- df_new_tmp
    } else {
      new_obs <- bind_rows(new_obs, df_new_tmp)
    }
    
  }
  
  return(new_obs)    
}
#END