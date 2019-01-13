# given a dataframe, class col and min class
# return a list with best ways to split numeric columns
describe_splits <- function(
  impurity_type = c("entropy", "gini", "minority_entropy", "minority_gini"), 
  data, 
  min_class = "min", 
  class_col = "class",
  bd_coefficient = 4, 
  cp_quantiles = c(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  use_n_cores = 1
) {

  require(dplyr)

  if(use_n_cores>1){
    require(parallel)

    split_cl <- parallel::makeCluster(use_n_cores)
    on.exit(stopCluster(split_cl))
    parallel::clusterExport(cl = split_cl, envir = environment(), 
                            varlist = c("data", ".generate_all_sequences", 
                                        ".generate_bd_sequence", ".generate_cp_sequence", 
                                        ".impurity_measure", ".create_intervals"))
    parallel::clusterCall(cl = split_cl, lapply, c("dplyr"), 
                          function(x) library(x, character.only = TRUE))
    
    splits <- parLapplyLB(cl = split_cl, 
                          X=colnames(data)[colnames(data)!=class_col], 
                          fun=.find_split, 
                          data=data, 
                          min_class=min_class, 
                          class_col=class_col, 
                          impurity_type=impurity_type,
                          bd_coefficient=bd_coefficient,
                          cp_quantiles=cp_quantiles)
  } else {
    splits <- lapply(X=colnames(data)[colnames(data)!=class_col], 
                     FUN=.find_split, 
                     data=data, 
                     min_class=min_class, 
                     class_col=class_col, 
                     impurity_type=impurity_type,
                     bd_coefficient=bd_coefficient,
                     cp_quantiles=cp_quantiles)
  }

  return(bind_rows(splits))

}

.find_split <- function(
  impurity_type, 
  data, 
  min_class = "min", 
  class_col = "class",
  feature = "", 
  bd_coefficient=4, 
  cp_quantiles = c(0.01, seq(from = 0.05, to = 0.95, by = 0.05), 0.99)) {
  
  if(feature %in% c("tmp_interval_col", "class_col", "n")) {
    stop("feature cannot have one of following names: \"tmp_interval_col\", \"class_col\", \"n\", \"pr_imp\"")
  }

  if(length(impurity_type)>1) {
    warning("impurity_measure: more than one type selected. Using entropy default.")
    impurity_type <- "entropy"
  }
  
  require(rlang)
  require(dplyr)
  
  data <- data %>%
    select(one_of(class_col, feature)) %>%
    mutate(class_col = ifelse(!!sym(class_col)==min_class, "min", "maj"))
  
  if(!class(unlist(data[,feature])) %in% c("factor", "character")) {
    
    all_combinations <- .generate_all_sequences(x = unlist(data[,feature]), 
                                               z = bd_coefficient, 
                                               probs = cp_quantiles)
    
    all_combinations <- all_combinations %>%
      mutate(imp_top=.impurity_measure(unlist(data[,"class_col"]), type = impurity_type), 
             imp_bot=NA, n_intervals=NA)
    
    for (comb in seq_len(nrow(all_combinations))) {
      cp_tmp <- all_combinations[comb, "cp"]
      bd_tmp <- all_combinations[comb, "bd"]
      
      intervals <- sort(.create_intervals(central_point = cp_tmp, 
                                         base_distance = bd_tmp,
                                         extremes = c(min(unlist(data[,feature])), max(unlist(data[,feature])))
      ))
      
      data <- data %>%
        mutate(tmp_interval_col = findInterval(x = !!sym(feature), vec = intervals))
      
      data_sum <- data %>%
        ungroup()
      
      if(grepl("minority", impurity_type)) {
        data_sum <- data_sum %>%
          filter(class_col == "min") 
      } 
      
      data_sum <- data_sum %>%
        group_by(tmp_interval_col) %>%
        summarise(
          n=n(),
          imp_indiv=.impurity_measure(class_col, type = impurity_type)
        ) %>%
        ungroup() %>%
        mutate(pr_imp = n / sum(n)) %>%
        mutate(imp_mult = pr_imp * imp_indiv) %>%
        summarise(imp_tot_bot = sum(imp_mult)) %>%
        pull(imp_tot_bot)
      
      all_combinations[comb, "imp_bot"]  <- data_sum
      all_combinations[comb, "n_intervals"] <- n_distinct(data[,"tmp_interval_col"])
    }
  } else {

      all_combinations <- data_frame(cp=NA, bd=NA, imp_top=NA, imp_bot=NA, n_intervals=NA)
      data <- data %>% 
        ungroup() %>%
        rename(tmp_interval_col = !!sym(feature)) 
      
      data_sum <- data %>%
        group_by(tmp_interval_col) %>%
        summarise(n=n(),
                  imp_indiv=.impurity_measure(class_col, type = impurity_type)
        ) %>%
        ungroup() %>%
        mutate(pr_imp=n/sum(n)) %>%
        mutate(imp_mult = pr_imp * imp_indiv) %>%
        summarise(imp_tot_bot=sum(imp_mult)) %>%
        pull(imp_tot_bot)
      
      all_combinations[1, "imp_top"]  <- .impurity_measure(unlist(data[,"class_col"]), type = impurity_type)
      all_combinations[1, "imp_bot"]  <- data_sum
      all_combinations[1, "n_intervals"] <- n_distinct(data[,"tmp_interval_col"])
  }

  all_combinations <- all_combinations %>%
    mutate(ig=imp_top-imp_bot) %>%
    mutate(impurity_type=impurity_type) %>%
    mutate(feature_name=feature)

  return(all_combinations)
}

# .impurity_measure: given a dataframe with column "class_col" E {min, maj} 
# what is the impurity measure of the column
# https://www3.nd.edu/~nchawla/papers/SDM10.pdf
.impurity_measure <- function(x, 
                             type=c("entropy", 
                                    "gini",
                                    "minority_entropy",
                                    "minority_gini"
                             ),
                             min_class="min") {
  
  require(dplyr)
  
  if(length(type)>1) {
    warning("impurity_measure: more than one type selected. Using entropy default.")
    type <- "entropy"
  }
  
  if(grepl("entropy", type)) {
    
    k <- - (x %>%
              data_frame(class_col=x) %>%
              count(class_col) %>%
              ungroup() %>% 
              mutate(p_c=n/sum(n)) %>% 
              mutate(log2p_c=base::log2(p_c)) %>% 
              mutate(product_pc_log2=p_c*log2p_c) %>%
              summarise(z=sum(product_pc_log2)) %>%
              pull(z))
    
  } else if(grepl("gini", type) & !grepl("minority", type)) {
    
    k <- 1 - (x %>%
                data_frame(class_col=x) %>%
                count(class_col) %>%
                ungroup() %>%
                mutate(p_c=n/sum(n)) %>%
                mutate(p_c_sqrd=p_c^2) %>% 
                summarise(z=sum(p_c_sqrd)) %>%
                pull(z))
    
    
  } else if (grepl("minority_gini", type)) {
    
    k <- 1 - (x %>%
                data_frame(class_col=x) %>%
                count(class_col) %>%
                filter(class_col==min_class) %>%
                mutate(p_c=n/sum(n)) %>%
                mutate(p_c_sq=p_c^2) %>%
                pull(p_c_sq))
    
    if(length(k)==0) {
      k <- 0
    }
    
  } else if(grepl("minority_entropy", type)) {
    
    k <- - (x %>%
              data_frame(class_col=x) %>%
              count(class_col) %>%
              filter(class_col=min_class) %>% 
              mutate(p_c=n/sum(n)) %>% 
              mutate(log2p_c=base::log2(p_c)) %>% 
              mutate(product_pc_log2=p_c*log2p_c) %>%
              pull(z))
    
  } else {
    stop("unknown type selected for information gain calculation")
  }
  return(k)
} 

# .create_intervals: given a numeric vector, return a same-length vector 
# that assigns each vector element to the intervals that cp and bd define
# @params result_type: should a vector or a dataframe be returned
# (seq_name, min_value and max_value describe each interval as row entry)
# @params central_point: central point through which interval should run through
# @params base_distance: base distance
# @params extremes: vector of length 2 giving the two extremes of the intervals. 
# @closed_side: when returning a data frame, should the seq name be open on the left or right side.
.create_intervals <- function(
  result_type = c("frame", "vector"),
  central_point = 1L,
  base_distance = 1L,
  extremes = c(0, 1),
  open_side = "left"
) { 
  if (length(extremes)>2 | length(extremes)<2) {
    stop("Only exactly two values in extremes vector allowed")
  }
  if (base_distance==0) {
    stop("base distance can only be non-zero")
  }
  if (base_distance<0) {
    base_distance <- abs(base_distance)
  }
  
  min_extr <- min(extremes)
  max_extr <- max(extremes)
  
  if (min_extr==max_extr) {
    stop("Only two non-equal values allowed as extremes")
  }
  
  mult_left <- floor((central_point - min_extr)/base_distance)
  mult_right <- floor((max_extr - central_point)/base_distance)
  
  if (min_extr >= central_point) {
    mult_left <- 0
  } else if (central_point >= max_extr) {
    mult_right <- 0
  }
  
  seq_start <- central_point - (mult_left + 1) * base_distance
  seq_end   <- central_point + (mult_right + 1) * base_distance
  seq_total <- seq(from = seq_start, to = seq_end, by = base_distance)
  
  if(length(result_type)==2) result_type <- "vector"
  if(result_type=="vector") {
    res_seq_total <- seq_total
  } else if(grepl("frame", result_type)) {
    side1 <- "("
    side2 <- "]"
    if(open_side=="right") {
      side1 <- "["
      side2 <- ")"
    }
    res_seq_total <- data.frame(seq_name = paste0(side1, seq_total[-length(seq_total)], ";", seq_total[-1], side2),
                                min_value = seq_total[-length(seq_total)],
                                max_value = seq_total[-1])
  }
  
  return(res_seq_total)
}

# from 1/z to z by 1
.generate_bd_sequence <- function(z, by = 1) {
  seq1 <- seq(from = 0, to = z, by = by)
  seq2 <- 1/seq1
  k <- sort(unique(c(seq1, seq2)))
  k <- k[!(k==0 | is.infinite(k))]
  return(k)
}

.generate_cp_sequence <- function(x, type=c("mean"), qprobs) {
  res <- c()
  if ("mean" %in% type) res <- c(res, mean(x))
  if ("median" %in% type) res <- c(res, mean(x))
  if ("min" %in% type) res <- c(res, mean(x))
  if ("max" %in% type) res <- c(res, mean(x))
  if ("quantile" %in% type) res <- c(res, unname(quantile(x, probs = qprobs)))
  res <- sort(unique(res))
  return(res)
}

.generate_all_sequences <- function(x, z, probs=seq(from = 0, to = 1, by = 0.05)) {
  a <- expand.grid(bd = .generate_bd_sequence(z = z)*sd(x),
                   cp = .generate_cp_sequence(x, type = c("mean", "quantile"), qprobs = probs))
  return(a)
}