setwd("~/ThinkBig/bootcamp/bootcamp-Jan-2017/vagrant/SkynetProject/data")

# LOAD data
reference <- read.csv("ACFTREF.txt", stringsAsFactors = FALSE)
master <- read.csv("MASTER.txt")

# removing non-commercial aircraft types from table
# 1 - Glider
# 2 - Balloon
# 3 - Blimp/Dirigible
# 4 - Fixed wing single engine 
# 5 - Fixed wing multi engine
# 6 - Rotorcraft
# 7 - Weight-shift-control
# 8 - Powered Parachute
# 9 - Gyroplane
reference <- subset(reference, TYPE.ACFT %in% c(4,5))

reference$SHORT.CODE <- substr(reference$X...CODE, 1, 5)
master$SHORT.CODE <- substr(master$MFR.MDL.CODE, 1, 5)

top_planes <- read.csv("top_planes.csv")

models <- reference[reference$SHORT.CODE %in% top_planes$SHORT.CODE, ]

# boeing
boeing = '^BOEING.*' 
models[grep(boeing, models$MFR, ignore.case = TRUE), ]$MFR <- c("BOEING")

# dougles
douglas = '^.*DOUGLAS.*'
models[grep(douglas, models$MFR, ignore.case = TRUE), ]$MFR <- c("DOUGLAS")

# bombardier
bombardier = '^.*BOMBARDIER.*'
models[grep(bombardier, models$MFR, ignore.case = TRUE), ]$MFR <- c("BOMBARDIER")

# cessna
cessna = '^.*CESSNA.*'
models[grep(cessna, models$MFR, ignore.case = TRUE), ]$MFR <- c("CESSNA")

# EMBRAER
embraer = '^.*EMBRAER.*'
models[grep(embraer, models$MFR, ignore.case = TRUE), ]$MFR <- c("EMBRAER")

# CANADAIR
canadair = '^.*CANADAIR.*'
models[grep(canadair, models$MFR, ignore.case = TRUE), ]$MFR <- c("CANADAIRD")

# AIRBUS
airbus = '^.*AIRBUS.*'
models[grep(airbus, models$MFR, ignore.case = TRUE), ]$MFR <- c("AIRBUS")


models$GMODEL <- c(NA)

models[models$MFR == "BOEING",]$GMODEL <- substr(models[models$MFR == "BOEING",]$MODEL, 1, 3)
models[models$MFR == "CESSNA",]$GMODEL <- substr(models[models$MFR == "CESSNA",]$MODEL, 1, 3)
models[models$MFR == "AIRBUS",]$GMODEL <- substr(models[models$MFR == "AIRBUS",]$MODEL, 1, 4)
models[models$MFR == "DOUGLAS",]$GMODEL <- substr(models[models$MFR == "DOUGLAS",]$MODEL, 1, 4)
models[models$MFR == "BOMBARDIER",]$GMODEL <- substr(models[models$MFR == "BOMBARDIER",]$MODEL, 1, 4)
models[models$MFR == "EMBRAER",]$GMODEL <- substr(models[models$MFR == "EMBRAER",]$MODEL, 1, 7)

merged <- merge(models, top_planes, by="SHORT.CODE")

library(ggplot2)

model_names <- head(names(sort(table(merged$GMODEL), decreasing = TRUE)))
ggplot(subset(merged, GMODEL %in% model_names), aes(x=GMODEL)) + geom_histogram(stat = "count")

ggplot(subset(merged, GMODEL %in% model_names), aes(x=GMODEL, fill = factor(MFR))) +
  geom_bar(stat = "count") + labs(title = ":)", x="PLANES", y="COUNT") +
  guides(fill=guide_legend(title="Manufacturer"))

airplanes_planes <- read.csv("airlines_planes.csv", stringsAsFactors = FALSE)

airlines_factor <- airplanes_planes[,c("AIRLINE", "COUNT")]
airlines_factor <- aggregate(. ~ AIRLINE, airlines_factor, sum)
factor <- airlines_factor[order(airlines_factor$COUNT, decreasing = TRUE), ]$AIRLINE


airplanes_planes <- airplanes_planes[order(airplanes_planes$COUNT),]
airplanes_planes$AIRLINE <- factor(airplanes_planes$AIRLINE, levels = factor)
ggplot(airplanes_planes, aes(x=AIRLINE, y=COUNT, fill = factor(PLANE))) + geom_bar(stat = "identity") + 
  theme_bw()+
   labs(title = "Airlines ", x="Airlines", y="Total number of planes") +
  guides(fill=guide_legend(title="Plane model")) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  scale_fill_brewer(palette="Set1") 

planes <- read.csv("popular_planes.csv", stringsAsFactors = FALSE)
planes$PLANE <- factor(planes$PLANE, levels = planes$PLANE)
planes$TYPE <- NA
planes[(1:3),]$TYPE <- c("Top 3 planes")
planes[(4:12),]$TYPE <- c("Rest")
planes$TYPE <- factor(planes$TYPE, levels=c("Top 3 planes", "Rest"))

ggplot(planes, aes(x=PLANE, y=COUNT, fill = factor(MANUFACTURER))) + geom_bar(stat = "identity") + 
  theme_bw() +
  labs(title = "Most popular planes without ADS-B transponder ", x="Plane models", y="Total number of planes") +
  guides(fill=guide_legend(title="Manufacturer")) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  facet_grid(. ~ TYPE, scales = "free", space = "free") +
  scale_fill_brewer(palette="Set1") 





