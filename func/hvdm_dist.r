### TODOs: 
#### Categorical per observation distance sum
#### tryCatch to close clusters on error if use_n_cores>1

# HVDM fn
hvdm_dist <- function(data, colname_target, use_n_cores=0) {
  require(dplyr)
  require(tidyr)
  require(tidyselect)
  
  if(use_n_cores>1 & (!is.na(use_n_cores))){
    require(parallel)
  }
  
  features <- as_data_frame(data) %>%
    rename(temp = !!sym(colname_target)) %>%
    mutate(id=1:n()) %>%
    select(id, temp, everything()) %>%
    mutate_if(is.factor, as.character)
  
  # Feature preprocessing
  
  ### Need: better concept for creating distance matrix for categorical features - separate into category and numeric loop?
  feature_classes <- sapply(features, class)
  feature_types <- feature_classes
  feature_types[feature_types %in% c("numeric", "integer")] <- "n_diff_x"
  feature_types[feature_types %in% c("character")] <- "nvdm"
  feature_types[names(feature_types) %in% c("temp", "id")] <- NA
  
  features_num_names <- names(feature_types)[feature_types=="n_diff_x" & !is.na(feature_types)]

  if(length(features_num_names)>0) {
    sd_f <- apply(features[,features_num_names], 2, sd) %>% data_frame(feature_colname=names(.), sd=.)
    dist_num <- features %>%
      select(id, one_of(features_num_names)) %>% 
      gather(feature_colname, original_x, -id) %>%
      left_join(sd_f, by="feature_colname") %>%
      mutate(x=original_x/(4*sd)) %>%
      select(-sd, -original_x)
    rm(sd_f)
  } else {
    temp_name <- toString(sample(x = c(letters, LETTERS), 7, replace = T))
    dist_num <- data_frame(id=1:nrow(features), feature_colname=temp_name, x=0)
  }
  
  
  features_cat_names <- names(feature_types)[feature_types=="nvdm" & !is.na(feature_types)]
  if(length(features_cat_names)>0) {
    feature_cat_enc <- features %>% 
      select(id, temp, one_of(features_cat_names)) %>%
      gather(feature_colname, feature_level, -id, -temp)
    
    dist_cat <- feature_cat_enc %>%
      group_by(feature_colname, feature_level, temp) %>%
      summarise(N_axc=n()) %>%
      group_by(feature_colname, feature_level) %>%
      mutate(N_ax=sum(N_axc)) %>%
      mutate(P_axc=N_axc/N_ax) %>%
      select(-N_axc, -N_ax) %>%
      rename(temp_loop=temp) %>%
      right_join(feature_cat_enc %>% select(-temp), by = c("feature_colname", "feature_level"))
    rm(feature_cat_enc, features_cat_names)
  } else {
    temp_name <- toString(sample(x = c(letters, LETTERS), 7, replace = T))
    dist_cat <- data_frame(id=1:nrow(features), feature_colname=temp_name, temp_loop=0, feature_level=0, P_axc=0)
  }
  
  if(use_n_cores>1 & (!is.na(use_n_cores))){
    require(parallel)
    load_balance <- features %>%
      select(id) %>%
      mutate(local_id=rep_len(c(1:use_n_cores), length.out = n())) %>%
      split(., .$local_id)

    
    hvdm_cl <- parallel::makeCluster(use_n_cores)
    parallel::clusterExport(cl=hvdm_cl, envir = environment(), c("dist_num", "dist_cat", "load_balance"))
    parallel::clusterCall(cl = hvdm_cl, function() library(dplyr))

    
    hvdm_obs_cl_fun <- function(df_dist_num, df_dist_cat, list_load_balance, X) {
      load_balance_local <- load_balance[[X]] 
      for (obs_n in load_balance_local$id){
        
        dist_num_obs <- df_dist_num %>% 
          filter(id==obs_n) %>%
          rename(id_x=id) %>%
          left_join(df_dist_num %>% 
                      rename(y=x, id_y=id), by="feature_colname") %>% 
          mutate(ndiff_a=abs(x-y)) %>% 
          select(id_x, id_y, feature_colname, ndiff_a) 
        
        dist_cat_obs <- df_dist_cat %>% 
          filter(id==obs_n) %>%
          left_join(df_dist_cat %>% rename(P_ayc=P_axc), by=c("feature_colname", "temp_loop"), suffix=c("_x", "_y")) %>%
          mutate(P_azc=(P_axc-P_ayc)^2) %>%
          group_by(id_x, id_y, feature_colname, feature_level_x, feature_level_y) %>%
          summarise(ndiff_a=sqrt(sum(P_azc))) %>%
          ungroup() %>%
          select(-feature_level_x, -feature_level_y)
        
        df_dist_total_obs <- dist_num_obs %>%
          bind_rows(dist_cat_obs) %>%
          group_by(id_x, id_y) %>%
          summarise(hvdm=sqrt(sum(ndiff_a^2)))
        
        if(obs_n==load_balance_local$id[1]) {
          df_dist_total_worker <- df_dist_total_obs
        } else {
          df_dist_total_worker <- df_dist_total_worker %>% 
            bind_rows(df_dist_total_obs)
        }
      }
      return(df_dist_total_worker)
    }
    
    list_dist_total_obs <- parLapplyLB(cl = hvdm_cl, 
                                       X = 1:use_n_cores, 
                                       fun = hvdm_obs_cl_fun, 
                                       df_dist_num=dist_num, 
                                       df_dist_cat=dist_cat,
                                       list_load_balance=load_balance)
    
    stopCluster(hvdm_cl)
    
    dist_total_all <- list_dist_total_obs %>%
      bind_rows(.)
  } else {
    for(obs in 1:nrow(features)) {
      
      dist_num_obs <- dist_num %>% 
        filter(id==obs) %>%
        rename(id_x=id) %>%
        left_join(dist_num %>% 
                    rename(y=x, id_y=id), by="feature_colname") %>% 
        mutate(ndiff_a=abs(x-y)) %>% 
        select(id_x, id_y, feature_colname, ndiff_a) 
      
      dist_cat_obs <- dist_cat %>% 
        filter(id==obs) %>%
        left_join(dist_cat %>% rename(P_ayc=P_axc), by=c("feature_colname", "temp_loop"), suffix=c("_x", "_y")) %>%
        mutate(P_azc=(P_axc-P_ayc)^2) %>%
        group_by(id_x, id_y, feature_colname, feature_level_x, feature_level_y) %>%
        summarise(ndiff_a=sqrt(sum(P_azc))) %>%
        ungroup() %>%
        select(-feature_level_x, -feature_level_y)
      
      dist_total_obs <- dist_num_obs %>%
        bind_rows(dist_cat_obs) %>%
        group_by(id_x, id_y) %>%
        summarise(hvdm=sqrt(sum(ndiff_a^2)))
      
      if(obs==1) {
        dist_total_all <- dist_total_obs
      } else {
        dist_total_all <- dist_total_all %>%
          bind_rows(dist_total_obs)
      }
    }
  }
  
  return(dist_total_all)
}

# pima_hvdm_dist <- hvdm_dist(pima, "Class")
# pima_hvdm_dist <- hvdm_dist(pima, "Class", use_n_cores = 4)
# pimacattest_hvdm_dist <- hvdm_dist(pima_cat_test_hvdm, "Class", use_n_cores = 4)
