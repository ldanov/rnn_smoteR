# data_fr, lb_df_local, minority_label, numerical
source("./tests/fcnmv.r")
source("./tests/generate_rnnsmote_preprocessing.r")
lb_df_local <- lb_df[[1]]
list2env(settings_supplier, envir = environment())

df_num_orig <- .fcnmv(data_fr=data_fr,
                lb_df_local=lb_df_local,
                minority_label=minority_label,
                numerical=numerical)