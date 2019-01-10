#################################
####### hvdm_dist ###############
## Heterogeneous Value Difference Metric
## Wilson & Martinez (1997): Improved Heterogeneous Distance Functions, pp.8-9
## Categorical are handled by N2: normalized_ vdm2 a (x, y)
## data - data frame containing class column, features and a unique key key_id. 
## Missing values not yet supported
## colname_target - string with column name of class labels
## use_n_cores - should synth obs generation be parallelized; on how many cores (DEFAULT:1)
## Returns a dataframe with key_id_x, key_id_y, class_col_x, class_col_y, dist (distance) 
## for each combination of x and y from key_id
## Lyubomir Danov, 2018
#################################

### TODO: Categorical per observation distance sum
#### tryCatch to close clusters on error if use_n_cores>1
hvdm_dist <- function(data, colname_target, use_n_cores=1) {
  require(dplyr)
  require(tidyr)
  require(tidyselect)
  
  if(missing(data)) {
    stop("data arg not specified")
  }
  if (missing(colname_target)) {
    stop("target column arg not specified")
  } else if (!is.character(colname_target) | length(colname_target)!=1) {
    stop("target column arg must be a single string") 
  }

  if("class_col" %in% colnames(data) & colname_target!="class_col") {
    stop("only colname_target can be named class_col")
  }

  for (k in c("key_id_x", "key_id_y")) {
    if(k %in% colnames(data)) {
      stop(paste0("data cannot contain column named ", k))
    }
  }

  if(!"key_id" %in% colnames(data)) {
    warning("data does not contain key_id key column. It will be automatically generated by mutate(key_id=row_number())")
      data <- data %>% 
        mutate(key_id=row_number())
  }

  if(use_n_cores>1){
    require(parallel)
  }
  
  .hvdm_cl_dist_matrix <- function(df_dist_num, df_dist_cat, list_load_balance, X) {
    load_balance_local <- list_load_balance[[X]] %>%
      select(key_id)

    dist_num_obs <- df_dist_num %>% 
      right_join(load_balance_local, by="key_id") %>%
      rename(key_id_x=key_id) %>%
      left_join(df_dist_num %>% 
                  rename(y=x, key_id_y=key_id), by="feature_colname") %>% 
      mutate(ndiff_a=abs(x-y)) %>% 
      select(key_id_x, key_id_y, feature_colname, ndiff_a) 

    dist_cat_obs <- df_dist_cat %>% 
      right_join(load_balance_local, by="key_id") %>%
      # if any of the features (a) has different class occurences (c), then expand to all observed classes
      # P_axc = (N_axc / N_ax) = (0 / N_ax) = 0
      full_join(df_dist_cat %>% rename(P_ayc=P_axc), by=c("feature_colname", "class_col_loop"), suffix=c("_x", "_y")) %>%
      mutate(P_axc=ifelse(is.na(P_axc), 0, P_axc)) %>%
      mutate(P_ayc=ifelse(is.na(P_ayc), 0, P_ayc)) %>%
      mutate(P_azc=(P_axc-P_ayc)^2) %>%
      group_by(key_id_x, key_id_y, feature_colname) %>%
      summarise(ndiff_a=sqrt(sum(P_azc))) %>%
      ungroup() 

    df_dist_total_worker <- dist_num_obs %>%
      bind_rows(dist_cat_obs) %>%
      group_by(key_id_x, key_id_y) %>%
      summarise(dist=sqrt(sum(ndiff_a^2)))

    return(df_dist_total_worker)
  }
  

  
  features <- as_data_frame(data) %>%
    rename(class_col = !!sym(colname_target)) %>%
    select(key_id, class_col, everything()) %>%
    mutate_if(is.factor, as.character)
  
  data <- data %>%
    select(one_of(c("key_id", colname_target))) %>%
    mutate(key_id_x=key_id, key_id_y=key_id)
  
  # Feature preprocessing
  
  feature_classes <- sapply(features, class)
  feature_types <- feature_classes
  feature_types[feature_types %in% c("numeric", "integer")] <- "n_diff_x"
  feature_types[feature_types %in% c("character")] <- "nvdm"
  feature_types[names(feature_types) %in% c("class_col", "key_id")] <- NA
  
  features_num_names <- names(feature_types)[feature_types=="n_diff_x" & !is.na(feature_types)]
  features_cat_names <- names(feature_types)[feature_types=="nvdm" & !is.na(feature_types)]

# Preprocess numeric features  
  if(length(features_num_names)>0) {
    sd_f <- apply(features[,features_num_names], 2, sd) %>% data_frame(feature_colname=names(.), sd=.)
    dist_num <- features %>%
      select(key_id, one_of(features_num_names)) %>% 
      gather(feature_colname, original_x, -key_id) %>%
      left_join(sd_f, by="feature_colname") %>%
      mutate(x=original_x/(4*sd)) %>%
      select(-sd, -original_x)
  
  } else {
    temp_name <- toString(sample(x = c(letters, LETTERS), 7, replace = T))
    dist_num <- data_frame(key_id=1:nrow(features), feature_colname=temp_name, x=0)
  }
  
# Preprocess categorical features
  if(length(features_cat_names)>0) {
    feature_cat_enc <- features %>% 
      select(key_id, class_col, one_of(features_cat_names)) %>%
      gather(feature_colname, feature_level, -key_id, -class_col)
    
    dist_cat <- feature_cat_enc %>%
      group_by(feature_colname, feature_level, class_col) %>%
      summarise(N_axc=n()) %>%
      group_by(feature_colname, feature_level) %>%
      mutate(N_ax=sum(N_axc)) %>%
      mutate(P_axc=N_axc/N_ax) %>%
      select(-N_axc, -N_ax) %>%
      rename(class_col_loop=class_col) %>%
      right_join(feature_cat_enc %>% select(-class_col), by = c("feature_colname", "feature_level"))

  } else {
    temp_name <- toString(sample(x = c(letters, LETTERS), 7, replace = T))
    dist_cat <- data_frame(key_id=1:nrow(features), feature_colname=temp_name, class_col_loop=0, feature_level=0, P_axc=0)
  }
  
  load_balance <- features %>%
    select(key_id) %>%
    mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
    split(., .$local_id)
  
  if(use_n_cores>1){
    
    hvdm_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl=hvdm_cl, envir = environment(), varlist = c("dist_num", "dist_cat", "load_balance"))
    parallel::clusterCall(cl = hvdm_cl, function() library(dplyr))
    
    list_dist_total_obs <- parLapplyLB(cl = hvdm_cl, 
                                       X = 1:use_n_cores, 
                                       fun = .hvdm_cl_dist_matrix, 
                                       df_dist_num=dist_num, 
                                       df_dist_cat=dist_cat,
                                       list_load_balance=load_balance)
    
    stopCluster(hvdm_cl)
   
  } else {
    
    list_dist_total_obs <- lapply(X = 1:use_n_cores,  
                                  FUN = .hvdm_cl_dist_matrix, 
                                  df_dist_num=dist_num, 
                                  df_dist_cat=dist_cat,
                                  list_load_balance=load_balance)
    
  }
  
  dist_total_all <- list_dist_total_obs %>%
    bind_rows(.) %>% 
    left_join(data %>% transmute(key_id_x=key_id, class_col_x=!!sym(colname_target)), by="key_id_x") %>%
    left_join(data %>% transmute(key_id_y=key_id, class_col_y=!!sym(colname_target)), by="key_id_y") %>%
    arrange(key_id_x, key_id_y) %>%
    as.data.frame(., row_names=NULL) 
  
  return(dist_total_all)
}

# pima_hvdm_dist <- hvdm_dist(pima, "Class");beepr::beep(2)
# pima_hvdm_dist2 <- hvdm_dist(pima, "Class", use_n_cores = 4) %>% arrange(key_id_x, key_id_y) %>% as.data.frame(.);beepr::beep(2)
# identical(pima_hvdm_dist, pima_hvdm_dist2)
# pima_hvdm_dist_cattest <- hvdm_dist(pima_cat_test_hvdm, "Class");beepr::beep(2)
# pima_hvdm_dist_cattest2 <- hvdm_dist(pima_cat_test_hvdm, "Class", use_n_cores = 4) %>% arrange(key_id_x, key_id_y) %>% as.data.frame(.);beepr::beep(2)
# identical(pima_hvdm_dist_cattest, pima_hvdm_dist_cattest2)

# compare_res <- microbenchmark(times = 10, control = list(order="block"), unit="t",
#                               hvdm_dist_cl01=hvdm_dist(colname_target = "Class", use_n_cores = 1, pima[,]),
#                               hvdm_dist_cl04=hvdm_dist(colname_target = "Class", use_n_cores = 4, pima[,]),
#                               hvdm_dist_cl08=hvdm_dist(colname_target = "Class", use_n_cores = 8, pima[,]),
#                               hvdm_dist_cl16=hvdm_dist(colname_target = "Class", use_n_cores = 16, pima[,])
#                               ); beepr::beep(9)

#END