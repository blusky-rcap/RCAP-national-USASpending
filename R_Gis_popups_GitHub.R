#### Final Script in Spending data processing to add GIS attributes for easier interpretation #####

#This script reads in USA Spending data output from Python and adds number of awards and full award names ####

# (C) Lena Schlichting, Rural Community Assistance Partnership, 2024


rm(list=ls())   #clean workspace

#load libraries
library("readxl")
library("stringr")
library("stringi")

dat <- read.csv("C:\\Users\\yourpath\\pythonoutput.csv")

pr.names <- read_excel("C:\\Users\\yourpath\\Program Search_CFDAs.xlsx")

#add columns for number of awards and full award names
dat$nr_awards <- NA
dat$award_names <- NA

#loop for nr of awards:
for (i in 1:nrow(dat)){
  
  nr_awards <- length(which((dat[i,3:45]) != 0))
  
  dat$nr_awards[i] <- nr_awards

}

#find program names from columns
col.names <- colnames(dat)
fixed.names <- str_sub(col.names, 2)
CDFA_list <- fixed.names[3:46]

#fix missing 0s in CDFA_list ######################################
tmp5 <- which(nchar(CDFA_list) == 5)
CDFA_list[tmp5] <- paste(CDFA_list[tmp5], "0", sep="")

tmp4 <- which(nchar(CDFA_list) == 4)
CDFA_list[tmp4] <- paste(CDFA_list[tmp4], "00", sep="")

#################################################################
#fix missing 0s in pr.names new column
pr.names$CDFA.char <- as.character(pr.names$CFDA)

tmp5 <- which(nchar(pr.names$CDFA.char) == 5)
pr.names$CDFA.char[tmp5] <- paste(pr.names$CDFA.char[tmp5], "0", sep="")

tmp4 <- which(nchar(pr.names$CDFA.char) == 4)
pr.names$CDFA.char[tmp4] <- paste(pr.names$CDFA.char[tmp4], "00", sep="")


#loop through to add proper names: program name and agency under ######################################
for (i in 1:nrow(dat)){
  
  loc_awards <- which((dat[i,3:46]) != 0)
  CDFA_num <- CDFA_list[loc_awards]
  
  #find proper CDFA name in pr.names
  loc <- which(pr.names$CDFA.char %in% CDFA_num)
  
  full_name <-paste(pr.names$Program[loc], " - ", pr.names$agency[loc], collapse="; ")
  
  dat$award_names[i] <- full_name
  
}

#export results
write.csv(dat, "C:\\Users\\yourpath\\data_with_GISattributes.csv")
