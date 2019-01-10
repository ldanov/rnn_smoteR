#################################
####### rare Nearest Neighbour SMOTE 1 ###################
## data_origin - data frame containing only target and features and wihout NAs
## target - string of target column name
## target_label - which label is the minority for which synth obs are generated
## multiply_min - how many synthetic observations to generate for each existing minority obs
## use_n_cores - should synth obs generation be parallelized; on how many cores (DEFAULT:1)
## handle_categorical - how should factor or character columns be returned. Currently only returns character columns.
## k - number of nearest neighbours to evaluate on (odd number recommended)
## Returns a dataframe with only newly created observations
## String columns are returned as factors
## Lyubomir Danov, 2018
#################################

rnn_smote_v1 <- function(data, colname_target, minority_label, 
                  knn=3, multiply_min=1, use_n_cores=1, 
                  handle_categorical=c("character"), 
                  seed_use=NA, conservative=FALSE, b_shape1=1, b_shape2=3) {
  require(dplyr)
  # Currently does not handle any NAs
  if(anyNA(data)) {
    stop("data contains NA values")
  }
  if("data.table" %in% class(data)) {
    stop("data cannot be of class data.table. Convert to data.frame and try again.")
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

  data_fr <- data %>%
    mutate(key_id=row_number()) %>%
    rename(class_col = !!sym(colname_target)) %>%
    select(key_id, class_col, everything()) %>%
    mutate_if(is.factor, as.character) 
  
  dist_min <- comp_dist_metric(dplyr::filter(data_fr, class_col==minority_label), 
                             colname_target="class_col", 
                             use_n_cores=use_n_cores, 
                             n_batches=use_n_cores, 
                             dist_type=c("hvdm")) %>%
  dplyr::filter(key_id_x!=key_id_y) %>%
  group_by(key_id_x) %>%
  top_n(n=-knn, wt=dist) %>%
  rename(neigh=key_id_y, key_id=key_id_x) %>%
  select(key_id, neigh)

  lb_df <- dist_min %>%
    select(key_id) %>%
    mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
    left_join(dist_min, by="key_id") %>%
    split(., .$local_id)

  col_classes <- sapply(data_fr, class)
  categorical_cols <- col_classes[col_classes=="character" & 
                        (!names(col_classes) %in% c("class_col", "key_id"))]
  numerical_cols <- col_classes[col_classes!="character" & 
                        (!names(col_classes) %in% c("class_col", "key_id"))]
  
  settings_supplier <- list(multiply_min=multiply_min,
                            seed=seed_use,
                            categorical=names(categorical_cols),
                            numerical=names(numerical_cols),
                            minority_label=minority_label,
                            conservative=conservative, 
                            b_shape1=b_shape1, 
                            b_shape2=b_shape2)
  
  if(use_n_cores>1){
    
    smote_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl = smote_cl, envir = environment(), 
        varlist = c("settings_supplier", "lb_df", "data_fr", ".fcnmv"))
    parallel::clusterCall(cl = smote_cl, function() library(dplyr))
    parallel::clusterCall(cl = smote_cl, function() library(tidyr))
    on.exit(stopCluster(smote_cl))
    
    list_new_obs <- parLapplyLB(cl = smote_cl,
                                X = 1:use_n_cores,
                                fun = .rnn_smote_cl_fun,
                                settings_supplier=settings_supplier,
                                lb_df = lb_df,
                                data_fr=data_fr)
    
    
  } else {
    
    list_new_obs <- lapply(X = 1:use_n_cores,
                           FUN = .rnn_smote_cl_fun,
                           settings_supplier=settings_supplier,
                           lb_df = lb_df,
                           data_fr=data_fr)
    
  }
  
  df_new <- list_new_obs %>% 
    bind_rows(.) %>%
    mutate(!!sym(colname_target):=minority_label) %>%
    as.data.frame(., row_names=NULL) 

  return(df_new)
  # return(list(settings_supplier, data_fr, lb_df))
}

.fcnmv <- function(data_fr, lb_df_local, minority_label, numerical) {
  require(dplyr)
  require(tidyr)
  
  distinct_keys <- c( lb_df_local %>%
                        ungroup() %>%
                        distinct(key_id) %>% 
                        pull(key_id), 
                      data_fr %>% 
                        ungroup() %>%
                        dplyr::filter(class_col!=minority_label) %>% 
                        pull(key_id) 
  )
  
  df_num_all <- data_fr %>%
    select(key_id, class_col, one_of(numerical)) %>%
    dplyr::filter(key_id %in% distinct_keys)
  
  for (k in seq_along(numerical)) {
    
    df_num_tmp <- df_num_all %>%
      select(key_id, class_col, one_of(numerical[k])) %>%
      gather(feat_name, feat_value_num, -key_id, -class_col)
    
    breaks_tmp <- c(-Inf, 
                    df_num_tmp %>%
                      dplyr::filter(class_col==minority_label) %>%
                      distinct(feat_value_num) %>%
                      arrange(feat_value_num) %>%
                      pull(feat_value_num),
                    Inf)
    
    df_num_tmp <- df_num_tmp %>%
      mutate(feat_val_grps_left=cut(feat_value_num, breaks=breaks_tmp, include.lowest=FALSE, right=TRUE)) %>%
      mutate(feat_val_grps_right=cut(feat_value_num, breaks=breaks_tmp, include.lowest=FALSE, right=FALSE))
    
    df_num_new <- df_num_tmp %>%
      dplyr::filter(class_col==minority_label) %>%
      left_join(df_num_tmp %>% 
                  dplyr::filter(class_col!=minority_label) %>% 
                  select(feat_val_grps_left, feat_value_num), 
                by="feat_val_grps_left", 
                suffix=c("", "_maj_left")
      ) %>%
      mutate(feat_value_num_maj_left=ifelse(is.na(feat_value_num_maj_left), -Inf, feat_value_num_maj_left)) %>%
      group_by(key_id) %>%
      dplyr::filter(feat_value_num_maj_left==max(feat_value_num_maj_left)) %>%
      ungroup() %>%
      distinct(key_id, .keep_all=TRUE) %>%
      left_join(df_num_tmp %>% 
                  dplyr::filter(class_col!=minority_label) %>% 
                  select(feat_val_grps_right, feat_value_num), 
                by="feat_val_grps_right", 
                suffix=c("", "_maj_right")
      ) %>%
      mutate(feat_value_num_maj_right=ifelse(is.na(feat_value_num_maj_right), Inf, feat_value_num_maj_right)) %>%
      group_by(key_id) %>%
      dplyr::filter(feat_value_num_maj_right==min(feat_value_num_maj_right)) %>%
      ungroup() %>%
      distinct(key_id, .keep_all=TRUE) %>%
      select(-starts_with("feat_val_grps")) %>%
      as.data.frame(.)
    
    if(k==1) {
      df_num_orig <- df_num_new
    } else {
      df_num_orig <- bind_rows(df_num_orig, df_num_new)
    }
  }
  
  return(df_num_orig)
}

.rnn_smote_cl_fun <- function(X, settings_supplier, lb_df, data_fr) {
  lb_df_local <- lb_df[[X]] 
  list2env(settings_supplier, envir = environment())
  
  df_cat_orig <- lb_df_local %>%
    distinct(key_id) %>%
    left_join(data_fr, by="key_id") %>%
    select(key_id, one_of(categorical)) %>%
    gather(feat_name, feat_value_cat, -key_id)
  
  df_num_orig <- .fcnmv(data_fr=data_fr, 
                        lb_df_local=lb_df_local, 
                        minority_label=minority_label,
                        numerical=numerical)
  
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
      group_by(key_id, feat_name) %>%
      mutate(closest_value=case_when(feat_value_num_o > feat_value_num_n & feat_value_num_maj_left > feat_value_num_n ~ "maj_l" ,
                                     feat_value_num_o < feat_value_num_n & feat_value_num_maj_right < feat_value_num_n ~ "maj_r",
                                     TRUE ~ "neigh"
      )) %>%
      ungroup() %>%
      mutate(feat_value_use = case_when(closest_value=="maj_l" ~ feat_value_num_maj_left,
                                        closest_value=="maj_r" ~ feat_value_num_maj_right,
                                        TRUE ~ feat_value_num_n)) %>%
      mutate(gap_r=rbeta(n=n(), shape1=b_shape1, shape2=b_shape2)) %>%
      mutate(gap_l=1-gap_r) %>%
      mutate(gap_n=runif(n())) %>%
      mutate(gap=case_when(closest_value=="maj_l" & conservative==TRUE ~ gap_l,
                           closest_value=="maj_r" & conservative==TRUE ~ gap_r,
                           TRUE ~ gap_n)) 
    
    df_num_new <- df_num_new %>%
      mutate(diff=feat_value_use - feat_value_num_o) %>%
      mutate(feat_value_num_synth = feat_value_num_o + (gap*diff)) %>%
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