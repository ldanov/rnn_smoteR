SMOTE_borderline <- function(data_origin, target, k, multiply_danger) {
  
  target_label <- names(which.min(table(data_origin[,target])))
  target_origin <- drop(data_origin[,target])
  print(target_label)
  print(target)
  
  target_vars <- as.numeric(target_origin==target_label)
  
  data <- data_origin[,colnames(data_origin)!=target]
  for (cats in 1:ncol(data)) {
    if (class(data[,cats])=="character") data[,cats] <- as.factor(data[,cats])
    if (class(data[,cats])=="factor") data[,cats] <- as.numeric(data[,cats])
  }
  
  categociral <- colnames(data_origin)[apply(data_origin, 2, is.factor)]
  categociral <- categociral[categociral!=target]
  
  maj_neighbours <- c()
  for(i in 1:length(target_vars)) {
    if(target_vars[i]==1) {
      # print(data[i,])
      # print(drop(data[i, ]))
      # print(ncol(data_origin))
      data_sc <- scale(x=data, 
                       center = unname(unlist(data[i, ])), 
                       scale = c(apply(data, 2, max) - apply(data, 2, min)))
      
      if(exists("categorical")){
        for (cat in categorical) {
          data_sc[,cat] <- data_sc[,cat] != 0
        }
      }
      
      kNNs <- order(drop(data_sc^2 %*% rep(1, ncol(data_sc))))[2:(k+1)]
      # number of minorities within knn is sum(target_vars[kNNs])
      maj_neighbours[i] <- k - sum(target_vars[kNNs])
    } else {
      maj_neighbours[i] <- NA
    }
  }
  
  label <- ifelse(test = maj_neighbours==k,
                  yes = "noise",
                  no = ifelse(test = maj_neighbours>=k/2 & maj_neighbours<k,
                              yes = "danger", no = ifelse(test = maj_neighbours<k/2, "safe", "")))
  
  label[is.na(label)] <- ""
  data_res <- cbind(data_origin, maj_neighbours, label, stringsAsFactors=FALSE)
  
  data <- cbind(label, data, stringsAsFactors=FALSE)
  data <- data[data$label!="",-1]
  label_res <- label[label!=""]
  
  new <- matrix(nrow=multiply_danger*length(label_res), ncol=(ncol(data)))
  keeper <- c()
  for(i in 1:nrow(data)) {
    
    data_sc <- scale(x=data,
                     center = unname(unlist(data[i, ])),
                     scale = c(apply(data, 2, max) - apply(data, 2, min)))
    
    if(exists("categorical")){
      for (cat in categorical) {
        data_sc[,cat] <- data_sc[,cat] != 0
      }
    }
    
    kNNs <- order(drop(data_sc^2 %*% rep(1, ncol(data_sc))))[2:(k+1)]
    
    for(n in 1:multiply_danger) {
      if(label_res[i]=="danger") {
        keeper <- c(keeper, TRUE)
        print(paste0("i: ", i, " ; n: ", n, " ; isdanger: ", label_res[i]=="danger"))
        # select randomly one of the k NNs
        neig <- sample(1:k,1)
        # the attribute values of the generated case
        new[(i-1)*multiply_danger+n,] <- unname(unlist(data[i,]+ runif(1) * (data[kNNs[neig],] - data[i,])))
        
        if(exists("categorical")){
          for (cat in categorical)
            new[(i-1)*multiply_danger+n, cat] <- c(data[kNNs[neig], cat], data[i, cat])[1+round(runif(1),0)]
        }
      } else {
        
        print(paste0("i: ", i, " ; n: ", n, " ; isdanger: ", label_res[i]=="danger"))
        keeper <- c(keeper, FALSE)
        new[(i-1)*multiply_danger+n,] <- NA
        
      }
      
    } 
  }
  
  new <- as.data.frame(new)
  new <- new[keeper,]
  if(exists("categorical")){
    for (cat in categorical)
      new[,cat] <- factor(new[,cat],levels=1:nlevels(data_origin[,cat]),labels=levels(data_origin[,cat]))
  }
  
  new[,target] <- factor(rep(target_label,nrow(new)),levels=levels(data_origin[,target]))
  new[,"maj_neighbours"] <- NA
  new[,"label"] <- "synth"
  # print(str(new))
  # print(str(data_res))
  colnames(new) <- colnames(data_res)
  # data_list <- list(data_res, new)
  
  data_res <- rbind(data_res, new)
  
  
  return(data_res)
}