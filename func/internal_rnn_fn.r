.rnn_smote_cl_fun <- function(X, settings_supplier, lb_df, data_fr) {
lb_df_local <- lb_df[[X]] 
list2env(settings_supplier, envir = environment())

df_cat_orig <- lb_df_local %>%
    distinct(key_id) %>%
    left_join(data_fr, by="key_id") %>%
    select(key_id, one_of(categorical)) %>%
    gather(feat_name, feat_value_cat, -key_id)

df_num_orig <- .find_closest_nonmin_vals(data_fr=data_fr, 
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
                                TRUE ~ gap_n)) %>%
        mutate(diff=feat_value_use - feat_value_num_o) %>%
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