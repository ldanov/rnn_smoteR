source("./tests/generate_test_data.r")
source("./func/heom_dist.r")
data <- testfr
colname_target <- "class"
minority_label <- "min"
use_n_cores <- 1
knn <- 3
multiply_min <- 2
seed_use <- 1
conservative=FALSE
b_shape1=1
b_shape2=3

data_fr <- data %>%
    mutate(key_id=row_number()) %>%
    rename(class_col = !!sym(colname_target)) %>%
    select(key_id, class_col, everything()) %>%
    mutate_if(is.factor, as.character) 

dist_min <- heom_dist(data=data_fr %>%
    filter(class_col==minority_label), colname_target="class_col", use_n_cores=use_n_cores) %>%
    filter(key_id_x!=key_id_y) %>%
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