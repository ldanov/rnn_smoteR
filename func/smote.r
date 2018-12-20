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
## Categorical columns are returned as strings
## Lyubomir Danov, 2018
#################################

smote <- function(data_origin, target, target_label, k, multiply_min, use_n_cores=1, handle_categorical=c("character"), seed_use=NA) {
  require(dplyr)
  # Currently does not handle any NAs
  if(anyNA(data_origin)) {
    stop("data_origin contains NA values")
  }
  if("data.table" %in% class(data_origin)) {
    stop("data_origin cannot be of class data.table. Convert to data.frame and try again.")
  }

  if("class_col" %in% colnames(data) & colname_target!="class_col") {
    stop("only colname_target can be named class_col")
  }

  for (k in c("key_id")) {
    if(k %in% colnames(data)) {
      stop(paste0("data cannot contain column named ", k))
    }
  }
    
  if(is.na(seed_use)) {
      seed_use <- as.integer(Sys.time())
      warning(paste0("seed_use not set; automatically set to current unix time, ", seed_use))
  }
  
  if(use_n_cores>1){
    require(parallel)
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
        select(-one_of(categorical), -matches("class_col")) %>%
        gather(feat_name, feat_value_num, -key_id)

    for (mult in 1:multiply_min) {
        set.seed(mult*seed)
        lb_df_local_tmp <- lb_df_local %>%
            group_by(key_id) %>%
            sample_n(1)

        df_cat_new <- lb_df_local_tmp %>%
            left_join(data_fr %>% rename(neigh=key_id), by=c("neigh") %>%
            select(key_id, one_of(categorical)) %>%
            gather(feat_name, feat_value_cat, -key_id) %>%
            full_join(df_cat_orig, 
                by=c("key_id", "feat_name"), 
                suffix=c("_n", "_o")) %>%
            mutate(gap=runif(n())) %>%
            mutate(feat_value_cat_synth=ifelse(gap>0.5, feat_value_cat_n, feat_value_cat_o)) %>%
            select(key_id, feat_name, feat_value_cat_synth) %>%
            spread(feat_name, feat_value_cat_synth)

        df_num_new <- lb_df_local_tmp %>%
            left_join(data_fr %>% rename(neigh=key_id), by=c("neigh") %>%
            select(-one_of(categorical), -matches("class_col")) %>%
            gather(feat_name, feat_value_num, -key_id) %>%
            full_join(df_num_orig, 
                by=c("key_id", "feat_name"), 
                suffix=c("_n", "_o")) %>%
            mutate(gap=runif(n())) %>%
            mutate(diff=feat_value_num_n - feat_value_num_o) %>%
            mutate(feat_value_num_synth=feat_value_num_o + (gap*diff))) %>%
            select(key_id, feat_name, feat_value_cat_synth) %>%
            spread(feat_name, feat_value_cat_synth)

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

  data_origin <- data_origin %>%
    mutate(key_id=row_number()) %>%
    rename(class_col = !!sym(colname_target)) %>%
    select(key_id, class_col, everything()) %>%
    mutate_if(is.factor, as.character)
  # filter only minority class, remove class label column  
  data_fr <- data_origin[data_origin[,target]==target_label, ]
  
  dist_min <- heom_dist(data=data_fr, colname_target="class_col", use_n_cores=use_n_cores) %>%
    group_by(key_id_x) %>%
    top_n(n=-k, wt=dist) %>%
    rename(neigh=key_id_y, key_id=key_id_x) %>%
    select(key_id, neigh)

  lb_df <- dist_min %>%
    select(key_id) %>%
    mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
    left_join(dist_min, by="key_id") %>%
    split(., .$local_id)

  categorical_cols <- sapply(data_fr, class)
  categorical_cols <- categorical_cols[categorical_cols=="character" & 
                        (!names(categorical_cols) %in% c("class_col", "key_id"))]
  
  settings_supplier <- list(multiply_min=multiply_min,
                            seed=seed_use,
                            categorical=names(categorical_cols))
  
  if(use_n_cores>1){
    
    smote_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl = smote_cl, envir = environment(), varlist = c("settings_supplier", "lb_df", "data_fr"))
    parallel::clusterCall(cl = smote_cl, function() library(dplyr))
    parallel::clusterCall(cl = smote_cl, function() library(tidyr))
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