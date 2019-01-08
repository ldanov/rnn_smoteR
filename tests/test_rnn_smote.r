
source("./func/heom_dist.r")
source("./func/rnn_smote.r")

source("./tests/generate_test_data.r")
rnn_smote_v1(data=testfr, knn=3, colname_target="class", minority_label="min", use_n_cores=1, multiply_min=2, seed_use=1)
list2env(list(data=testfr, knn=3, colname_target="class", minority_label="min", use_n_cores=2, multiply_min=2, seed_use=1, conservative=FALSE, b_shape1=1, b_shape2=3), envir = environment())

source("./func/internal_rnn_fn.r")
source("./func/find_nonmin_values_backup.r")
.rnn_smote_cl_fun(X=1, settings_supplier=settings_supplier, lb_df=lb_df, data_fr=data_fr)