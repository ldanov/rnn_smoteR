# given a df with feat_name, feat_value_num, key_id
# return

.find_closest_nonmin_vals <- function(data_fr, lb_df_local, minority_label, numerical) {
    require(dplyr)
    require(tidyr)

    distinct_keys <- c( lb_df_local %>%
                            distinct(key_id) %>% 
                            pull(key_id), 
                        data_fr %>% 
                            filter(class_col!=minority_label) %>% 
                            pull(key_id) 
                    )

    df_num_all <- data_fr %>%
        select(key_id, class_col, one_of(numerical)) %>%
        filter(key_id %in% distinct_keys)

    for (k in seq_along(numerical)) {
        
        df_num_tmp <- df_num_all %>%
            select(key_id, class_col, one_of(numerical[k])) %>%
            gather(feat_name, feat_value_num, -key_id, -class_col)

        breaks_tmp <- c(-Inf, 
            df_num_tmp %>%
                filter(class_col==minority_label) %>%
                distinct(feat_value_num) %>%
                arrange(feat_value_num) %>%
                pull(feat_value_num),
            Inf)

        df_num_tmp <- df_num_tmp %>%
            mutate(feat_val_grps_left=cut(feat_value_num, breaks=breaks_tmp, include.lowest=FALSE, right=TRUE)) %>%
            mutate(feat_val_grps_right=cut(feat_value_num, breaks=breaks_tmp, include.lowest=FALSE, right=FALSE))

        df_num_new <- df_num_tmp %>%
            filter(class_col==minority_label) %>%
            left_join(df_num_tmp %>% 
                        filter(class_col!=minority_label) %>% 
                        select(feat_val_grps_left, feat_value_num), 
                      by="feat_val_grps_left", 
                      suffix=c("", "_maj_left")
                      ) %>%
            group_by(key_id) %>%
            filter(feat_value_num_maj_left==max(feat_value_num_maj_left)) %>%
            ungroup() %>%
            distinct(feat_value_num_maj_left, .keep_all=TRUE) %>%
            left_join(df_num_tmp %>% 
                        filter(class_col!=minority_label) %>% 
                        select(feat_val_grps_right, feat_value_num), 
                      by="feat_val_grps_right", 
                      suffix=c("", "_maj_right")
                      ) %>%
            group_by(key_id) %>%
            filter(feat_value_num_maj_right==min(feat_value_num_maj_right)) %>%
            ungroup() %>%
            distinct(feat_value_num_maj_right, .keep_all=TRUE) %>%
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