# NYC Parking Spark Assignment by DS Warriors(Gaurav Sachdeva,Meenakshi Lakshmanan,Sriram Pala, Parth)

# Essential commands

# Load SparkR
spark_path <- '/usr/local/spark'
+
  if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
    Sys.setenv(SPARK_HOME = spark_path)
  }

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))

# Reading  data

nyc_pkg_tkt_2017 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", "csv", header = "true", inferSchema = "true")

head(nyc_pkg_tkt_2017)
# Notice values from years other than 2017

nrow(nyc_pkg_tkt_2017)
#10803028 records

# Removing space from  the column names.Not a good practice
colnames(nyc_pkg_tkt_2017) <- str_replace_all(colnames(nyc_pkg_tkt_2017), pattern = " ", replacement = "")

#############Cleaning the data Starts##################

#Extracting Year
nyc_pkg_tkt_2017$yr<-year(nyc_pkg_tkt_2017$IssueDate)

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

# Creating temporary view to execute the SQL queries for further analysis
createOrReplaceTempView(nyc_pkg_tkt_2017, "nyc_pkg_tkt_2017_tbl")

#Filtering for data from year 2017
nyc_pkg_tkt_2017_cln<-SparkR::sql("SELECT * FROM nyc_pkg_tkt_2017_tbl where yr==2017 ")

head(nyc_pkg_tkt_2017_cln)

#Checking for NA rows.All entries NA

nyc_parking_2017_without_rowna <- dropna(nyc_pkg_tkt_2017_cln, how = c("all"))
nrow(nyc_parking_2017_without_rowna)#5431918
nrow(nyc_pkg_tkt_2017_cln)#5431918

# No NA rows found

#############Cleaning the data Ends##################

# 1. Finding the total number of tickets for fiscal year 2017
#------------------------------------------------------------

nrow(nyc_pkg_tkt_2017_cln)
#5431918 records

library(stringr)

# 2. Finding the number of distinct states for each year from where the cars got tickets
#----------------------------------------------------------------------------------------------

# Creating temporary view to execute the SQL queries for further analysis

createOrReplaceTempView(nyc_pkg_tkt_2017_cln, "nyc_pkg_tkt_2017_tbl")
nyc_pkg_tkt_2017_qry1<- SparkR::sql("SELECT distinct RegistrationState FROM nyc_pkg_tkt_2017_tbl where yr==2017")
head(nyc_pkg_tkt_2017_qry1)
nrow(nyc_pkg_tkt_2017_qry1)

#65 no of states but this contains value 99 also

nyc_pkg_tkt_2017_qry2<- SparkR::sql("SELECT RegistrationState,count(*) as Count_pkg_tkts FROM nyc_pkg_tkt_2017_tbl where yr==2017 group by RegistrationState order by Count_pkg_tkts desc ")
head(nyc_pkg_tkt_2017_qry2)
# NY IS STATE WITH MAX NO OF VIOLATIONS
nrow(nyc_pkg_tkt_2017_qry2)


nyc_pkg_tkt_2017_corr<-nyc_pkg_tkt_2017_cln

#CHANGING 99 TO NY
nyc_pkg_tkt_2017_corr$RegistrationState <- ifelse(nyc_pkg_tkt_2017_corr$RegistrationState == "99", 
                                                  "NY", nyc_pkg_tkt_2017_corr$RegistrationState)

# Creating temporary view to execute the SQL queries for further analysis
createOrReplaceTempView(nyc_pkg_tkt_2017_corr, "nyc_pkg_tkt_2017_tbl1")

#  Finding the number of distinct states after above operation'

nyc_pkg_tkt_2017_qry3<- SparkR::sql("SELECT distinct RegistrationState FROM nyc_pkg_tkt_2017_tbl1 ")
head(nyc_pkg_tkt_2017_qry3)
nrow(nyc_pkg_tkt_2017_qry3)

# NOW 64 STATES after removing 99 and replacing with NY

#----------------------------------------Aggregation Task-------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------

# 1. Displaying frequency of top five violation codes
#------------------------------------------------------------

nyc_pkg_tkt_2017_qry4<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 group by ViolationCode order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry4)

#   ViolationCode  COUNT                                                          
# 1            21 768087
# 2            36 662765
# 3            38 542079
# 4            14 476664
# 5            20 319646

# 2. Finding the top 5 Vehicle Body Type and Vehicle Make that got parking tickets
#----------------------------------------------------------------------------------

# Vehicle Body Type
#------------------

nyc_pkg_tkt_2017_qry5<- SparkR::sql("SELECT  VehicleBodyType,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 group by VehicleBodyType order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry5)

#   VehicleBodyType   COUNT                                                       
#1            SUBN 1883954
#2            4DSD 1547312
#3             VAN  724029
#4            DELV  358984
#5             SDN  194197

# Vehicle Make
#-------------

nyc_pkg_tkt_2017_qry6<- SparkR::sql("SELECT  VehicleMake,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 group by VehicleMake order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry6)

#   VehicleMake  COUNT                                                            
#1        FORD 636844
#2       TOYOT 605291
#3       HONDA 538884
#4       NISSA 462017
#5       CHEVR 356032




# 3. Finding the top 5 Violation Precinct and Issuer Precint where Violation occured and ticket being issued respectively
#------------------------------------------------------------------------------------------------------------------------

# Violation Precinct
#-------------------

nyc_pkg_tkt_2017_qry7<- SparkR::sql("SELECT  ViolationPrecinct,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 group by ViolationPrecinct order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry7)

#   ViolationPrecinct  COUNT                                                      
#1                 0 925596
#2                19 274445
#3                14 203553
#4                 1 174702
#5                18 169131

nyc_pkg_tkt_2017_qry8<- SparkR::sql("SELECT  ViolationPrecinct,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 where ViolationPrecinct!=0  group by ViolationPrecinct order by COUNT desc LIMIT 5 ")
head(nyc_pkg_tkt_2017_qry8)

#   ViolationPrecinct  COUNT                                                      
#1                19 274445
#2                14 203553
#3                 1 174702
#4                18 169131
#5               114 147444

# Issuer Precinct
#-------------------

nyc_pkg_tkt_2017_qry9<- SparkR::sql("SELECT  IssuerPrecinct,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 group by IssuerPrecinct order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry9)

#   IssuerPrecinct   COUNT                                                        
#1              0 1078406
#2             19  266961
#3             14  200495
#4              1  168740
#5             18  162994

# IssuerPrecinct has values equal to 0.Ignoring such values  as these are erronoues values

nyc_pkg_tkt_2017_qry10<- SparkR::sql("SELECT  IssuerPrecinct,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 where IssuerPrecinct!=0  group by IssuerPrecinct order by COUNT desc LIMIT 5")
head(nyc_pkg_tkt_2017_qry10)

#   IssuerPrecinct  COUNT                                                         
#1             19 266961
#2             14 200495
#3              1 168740
#4             18 162994
#5            114 144054

# 4. Finding the Violation Code frequency across three Precint Zones which have issued the most number of tickets 
#---Issuer Precinct 19,14,1 are considered for analysis

#Issuer Precinct=19

nyc_pkg_tkt_2017_qry11<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 where IssuerPrecinct==19  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry11)

# ViolationCode COUNT                                                           
#1            46 48445
#2            38 36386
#3            37 36056

#Issuer Precinct=14

nyc_pkg_tkt_2017_qry12<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 where IssuerPrecinct==14  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry12)

#   ViolationCode COUNT                                                           
#1            14 45036
#2            69 30464
#3            31 22555

#Issuer Precinct=1

nyc_pkg_tkt_2017_qry13<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl1 where IssuerPrecinct==1  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry13)

#   ViolationCode COUNT                                                           
#1            14 38354
#2            16 19081
#3            20 15408

# 5. Analysis on Parking Violations across different times of the day
#--------------------------------------------------------------------

# Checking for NA Values in Vehicle Make
nyc_pkg_tkt_2017_qry14<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(VehicleMake)==TRUE")
head(nyc_pkg_tkt_2017_qry14)# No NA Values found

# Checking for NA Values in Issue Date
nyc_pkg_tkt_2017_qry15<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(IssueDate)==TRUE")
head(nyc_pkg_tkt_2017_qry15)# No NA Values found

# Checking for NA Values in Registration State
nyc_pkg_tkt_2017_qry16<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(RegistrationState)==TRUE")
head(nyc_pkg_tkt_2017_qry16)# No NA Values found

# Checking for NA Values in SummonsNumber
nyc_pkg_tkt_2017_qry17<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(SummonsNumber)==TRUE")
head(nyc_pkg_tkt_2017_qry17)# No NA Values found

# Checking for NA Values in Plate ID
nyc_pkg_tkt_2017_qry18<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(PlateID)==TRUE")
head(nyc_pkg_tkt_2017_qry18)# No NA Values found

# Checking for NA Values in Vehicle Body Type
nyc_pkg_tkt_2017_qry19<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(VehicleBodyType)==TRUE")
head(nyc_pkg_tkt_2017_qry19)# No NA Values found

#checking and dropping NA for ViolationtTime

nyc_pkg_tkt_2017_qry19_1<- SparkR::sql("SELECT  COUNT(*) AS NULL_COUNT FROM nyc_pkg_tkt_2017_tbl1 WHERE ISNULL(ViolationTime)==TRUE")
head(nyc_pkg_tkt_2017_qry19_1)# No NA Values found

nyc_pkg_tkt_2017_corr1<- dropna(nyc_pkg_tkt_2017_corr, cols = "ViolationTime")
nrow(nyc_pkg_tkt_2017_corr1) # No NA values found for ViolationTime

#Extracting Hour from Vioaltion Time 
nyc_pkg_tkt_2017_corr1$HH<-cast(substr(nyc_pkg_tkt_2017_corr1$ViolationTime, 1, 2),"integer")

#Add 12 hrs for PM
nyc_pkg_tkt_2017_corr1$HH<-ifelse(substr(nyc_pkg_tkt_2017_corr1$ViolationTime, 5, 5) =="P",nyc_pkg_tkt_2017_corr1$HH+12,nyc_pkg_tkt_2017_corr1$HH)

#Extracting Minutes
nyc_pkg_tkt_2017_corr1$MM<-cast(substr(nyc_pkg_tkt_2017_corr1$ViolationTime, 3, 4),"integer") 

head(nyc_pkg_tkt_2017_corr1)

str(nyc_pkg_tkt_2017_corr1)

createOrReplaceTempView(nyc_pkg_tkt_2017_corr1, "nyc_pkg_tkt_2017_tbl2")

nyc_pkg_tkt_2017_corr_b4_bins<- SparkR::sql("SELECT ViolationTime,HH,ViolationCode FROM nyc_pkg_tkt_2017_tbl2")
createOrReplaceTempView(nyc_pkg_tkt_2017_corr_b4_bins, "nyc_pkg_tkt_2017_tbl3")

# Binning into different times of the day
bins <- sql("SELECT HH,ViolationTime,ViolationCode,\
            CASE  WHEN HH >= 0 and HH <= 3  THEN 'Late Night'\
            WHEN (HH >= 4 and HH <= 7) THEN 'Early Morning'\
            WHEN (HH >= 8 and HH <= 11) THEN 'Morning' \
            WHEN (HH >= 12 and HH <= 15) THEN 'Afternoon'\
            WHEN (HH >= 16 and HH <= 19) THEN 'Evening'\
            ELSE 'Late Evening' END  as bin_number FROM nyc_pkg_tkt_2017_tbl3")

#Creating Bin Numbers for easy plotting

bins_n <- sql("SELECT HH,ViolationTime,ViolationCode,\
              CASE  WHEN HH >= 0 and HH <= 3  THEN 1\
              WHEN (HH >= 4 and HH <= 7) THEN 2\
              WHEN (HH >= 8 and HH <= 11) THEN 3 \
              WHEN (HH >= 12 and HH <= 15) THEN 4\
              WHEN (HH >= 16 and HH <= 19) THEN 5\
              ELSE 6 END  as bin_number FROM nyc_pkg_tkt_2017_tbl3")

head(select(bins, c(bins$HH, bins$ViolationCode,bins$bin_number)))
head(select(bins_n, c(bins_n$HH, bins_n$ViolationCode,bins_n$bin_number)))

createOrReplaceTempView(bins, "nyc_pkg_tkt_2017_tbl4")

nyc_pkg_tkt_2017_qry20<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Late Night'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry20)

#   ViolationCode COUNT                                                           
#1            21 34704
#2            40 23629
#3            14 14168

nyc_pkg_tkt_2017_qry21<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Early Morning'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry21)

# ViolationCode COUNT                                                           
#1            14 74114
#2            40 60652
#3            21 57897

nyc_pkg_tkt_2017_qry22<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Morning'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry22)

#   ViolationCode  COUNT                                                          
#1            21 598070
#2            36 348165
#3            38 176570

nyc_pkg_tkt_2017_qry23<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Afternoon'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry23)

#   ViolationCode  COUNT                                                          
#1            38 184829
#2            36 184293
#3            37 130692

nyc_pkg_tkt_2017_qry24<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Evening'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry24)

#  violationCode  COUNT                                                          
#1            38 102855
#2            14  75902
#3            37  70345

nyc_pkg_tkt_2017_qry25<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where bin_number=='Late Evening'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry25)

#   ViolationCode  COUNT                                                          
#1            36 101991
#2            38  76314
#3            21  72568

# Checking the frequency in bins for Violation Code 21,36,38

# Finding the frequency of violations across each season for Violation Code  21
nyc_pkg_tkt_2017_qry26<- SparkR::sql("SELECT  bin_number,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where ViolationCode==21  group by bin_number order by COUNT desc")
head(nyc_pkg_tkt_2017_qry26)

#     bin_number  COUNT                                                          
#1       Morning 598070
#2  Late Evening  72568
#3 Early Morning  57897
#4    Late Night  34704
#5     Afternoon   4589
#6       Evening    259

hist <- histogram(bins_n, bins_n$bin_number, nbins = 12)

library(ggplot2)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("Time of the Day") + ylab("Violation Frequency")
plot

# Finding the frequency of violations across each season for Violation Code  36
nyc_pkg_tkt_2017_qry27<- SparkR::sql("SELECT  bin_number,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where ViolationCode==36  group by bin_number order by COUNT desc")
head(nyc_pkg_tkt_2017_qry27)

#     bin_number  COUNT                                                          
#1       Morning 348165
#2     Afternoon 184293
#3  Late Evening 101991
#4 Early Morning  14782
#5       Evening  13534

# Finding the frequency of violations across each season for Violation Code  38
nyc_pkg_tkt_2017_qry28<- SparkR::sql("SELECT  bin_number,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl4 where ViolationCode==38   group by bin_number order by COUNT desc")
head(nyc_pkg_tkt_2017_qry28)

#     bin_number  COUNT                                                          
#1     Afternoon 184829
#2       Morning 176570
#3       Evening 102855
#4  Late Evening  76314
#5 Early Morning   1273
#6    Late Night    238


# 6. Analysis on Parking Violations across different Seasons
#-----------------------------------------------------------

#Checking and dropping any NAs in Issue Date

nyc_pkg_tkt_2017_corr2<- dropna(nyc_pkg_tkt_2017_corr, cols = "IssueDate")
nrow(nyc_pkg_tkt_2017_corr2) 
#Since number of rows are equal no NAs 

# Extracting month from Issue Date
nyc_pkg_tkt_2017_corr2$Month<-month(nyc_pkg_tkt_2017_corr2$IssueDate)

str(nyc_pkg_tkt_2017_corr2)

createOrReplaceTempView(nyc_pkg_tkt_2017_corr2, "nyc_pkg_tkt_2017_tbl5")

# Creating View from Month,Violation Code
nyc_pkg_tkt_2017_corr_b4_seasons<- SparkR::sql("SELECT Month,ViolationCode FROM nyc_pkg_tkt_2017_tbl5")
createOrReplaceTempView(nyc_pkg_tkt_2017_corr_b4_seasons, "nyc_pkg_tkt_2017_tbl6")

# Binning into different seasons

# Spring Season - March, April and May
# Summer Season - June, July and August
# Fall Season - September, October and November
# Winter Season - Dec, Jan and Feb

bins_season <- sql("SELECT Month,ViolationCode,\
                   CASE  WHEN Month >= 3 and Month <= 5  THEN 'Spring'\
                   WHEN (Month >= 6 and Month <= 8) THEN 'Summer'\
                   WHEN (Month >= 9 and Month <=11 ) THEN 'Fall' \
                   ELSE 'Winter' END  as Season FROM nyc_pkg_tkt_2017_tbl6")

#creating bins with numbers

bins_season_n <- sql("SELECT Month,ViolationCode,\
                     CASE  WHEN Month >= 3 and Month <= 5  THEN 1\
                     WHEN (Month >= 6 and Month <= 8) THEN 2\
                     WHEN (Month >= 9 and Month <=11 ) THEN 3\
                     ELSE 4 END  as Season FROM nyc_pkg_tkt_2017_tbl6")

head(select(bins_season, c(bins_season$Month, bins_season$ViolationCode,bins_season$Season)))

head(select(bins_season_n, c(bins_season_n$Month, bins_season_n$ViolationCode,bins_season_n$Season)))

createOrReplaceTempView(bins_season, "nyc_pkg_tkt_2017_tbl7")

# Finding the total frequency of violations across each season

nyc_pkg_tkt_2017_qry29<- SparkR::sql("SELECT  Season,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 group by Season order by COUNT desc")
head(nyc_pkg_tkt_2017_qry29)

# Season   COUNT                                                                
#1 Spring 2873383
#2 Winter 1704690
#3 Summer  852866
#4   Fall     979

hist1 <- histogram(bins_season_n, bins_season_n$Season, nbins = 12)

plot1 <- ggplot(hist1, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("Season") + ylab(" Violation Frequency")
plot1

# Finding out the most common vioaltion codes in different seasons

#Spring
nyc_pkg_tkt_2017_qry34<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where Season=='Spring' group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry34)

#   ViolationCode  COUNT                                                          
#1            21 402424
#2            36 344834
#3            38 271167

#Summer
nyc_pkg_tkt_2017_qry35<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where Season=='Summer'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry35)

#   ViolationCode  COUNT                                                          
#1            21 127352
#2            36  96663
#3            38  83518

#Fall
nyc_pkg_tkt_2017_qry36<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where Season=='Fall'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry36)

#   ViolationCode COUNT                                                           
#1            46   231
#2            21   128
#3            40   116

#Winter
nyc_pkg_tkt_2017_qry37<- SparkR::sql("SELECT  ViolationCode,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where Season=='Winter'  group by ViolationCode order by COUNT desc LIMIT 3")
head(nyc_pkg_tkt_2017_qry37)

#   ViolationCode  COUNT                                                          
# 1            21 238183
# 2            36 221268
# 3            38 187386

# Since Violations for ViolationCode 21,36,38 are highest they are considered for analysis

# Finding the frequency of violations across each season for Violation Code  21

nyc_pkg_tkt_2017_qry30<- SparkR::sql("SELECT  Season,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where ViolationCode==21 group by Season order by COUNT desc")
head(nyc_pkg_tkt_2017_qry30)

# Season  COUNT                                                                 
#1 Spring 402424
#2 Winter 238183
#3 Summer 127352
#4   Fall    128

# Finding the frequency of violations across each season for Violation Code  36

nyc_pkg_tkt_2017_qry31<- SparkR::sql("SELECT  Season,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where ViolationCode==36 group by Season order by COUNT desc")
head(nyc_pkg_tkt_2017_qry31)

# Season  COUNT                                                                 
#1 Spring 344834
#2 Winter 221268
#3 Summer  96663

# Finding the frequency of violations across each season for Violation Code 38.

nyc_pkg_tkt_2017_qry32<- SparkR::sql("SELECT  Season,count(*) as COUNT FROM nyc_pkg_tkt_2017_tbl7 where ViolationCode==38  group by Season order by COUNT desc")
head(nyc_pkg_tkt_2017_qry32)

# Season  COUNT                                                                 
#1 Spring 271167
#2 Winter 187386
#3 Summer  83518
#4   Fall      8


# 7. Estimating the revenue for top 3 violation codes with maximum tickets
#-------------------------------------------------------------------------

nyc_pkg_tkt_2017_qry33<-SparkR::sql("SELECT ViolationCode, count(*) as frequency from nyc_pkg_tkt_2017_tbl1 group by ViolationCode order by frequency desc limit 3")
head(nyc_pkg_tkt_2017_qry33)
freq_data<-head(nyc_pkg_tkt_2017_qry33)

#   ViolationCode frequency                                                       
#1            21    768087
#2            36    662765
#3            38    542079

# As checked on NYC website, Violation code 21, 36,38 have separate fines for Manhattan vs other areas.Calculating Avaerage

#Avg fine for Violation Code 21

Avg_fine_21<-(65+45)/2
Avg_fine_21

#Avg fine for Violation Code 36

Avg_fine_36<-(50+50)/2
Avg_fine_36

#Avg fine for Violation Code 36

Avg_fine_38<-(65+35)/2
Avg_fine_38

## Calculating the total amount of money collected for these violation codes

#Total fine collected for Code 21

total_fine_21<-freq_data[1,2]*Avg_fine_21
total_fine_21#42244785 dollars(approx 42.25 Mil$)

#Total fine collected for Code 36

total_fine_36<-freq_data[2,2]*Avg_fine_36
total_fine_36 #33138250 dollars(approx 33.13 Mil$)

#Total fine collected for Code 38

total_fine_38<-freq_data[3,2]*Avg_fine_38
total_fine_38 #27103950 dollars(approx 27.1 Mil$)