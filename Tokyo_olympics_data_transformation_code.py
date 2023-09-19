# Databricks notebook source
from pyspark.sql.functions import col, corr
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------


configs = {"": "OAuth",
"fs.azure.account.oauth.provider.type": "",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": ""}


dbutils.fs.mount(
source = "", 
mount_point = "/mnt/tokyoolymics",
extra_configs = configs)
  

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymics"

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymics/Raw-Data/athletes")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymics/Raw-Data/coaches")
entriesgender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymics/Raw-Data/entriesgender")
medals = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymics/Raw-Data/medals")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymics/Raw-Data/teams")

# COMMAND ----------

# ATHLETES TRANSFORMATION

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg

# Transformation: Calculate the athlete count per discipline per country
athlete_count_per_discipline_per_country = athletes.groupBy("Country", "Discipline").agg(countDistinct("PersonName").alias("AthleteCountPerDisciplinePerCountry"))

# Transformation: Calculate the average athlete count per discipline per country
avg_athlete_count_per_discipline_per_country = athlete_count_per_discipline_per_country.groupBy("Country").agg(
    countDistinct("Discipline").alias("DisciplineCount"),
    avg("AthleteCountPerDisciplinePerCountry").alias("AvgAthleteCountPerDisciplinePerCountry")
)

# Transformation: Calculate the athlete count per country
athlete_count_per_country = athletes.groupBy("Country").agg(countDistinct("PersonName").alias("AthleteCountPerCountry"))

athletes = avg_athlete_count_per_discipline_per_country.join(athlete_count_per_country, on=["Country"], how="left")
athletes.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolymics/Transformed-Data/athletes")

athletes.show()
athletes.printSchema()






# COMMAND ----------

athletes.printSchema()
athletes.show()

# COMMAND ----------

##COACHES TRANSFORMATION

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Transformation: Calculate the count of coaches per country per discipline
coaches_per_country_per_discipline = coaches.groupBy("Country", "Discipline").agg(count("Name").alias("CoachCountPerCountryPerDiscipline"))

# Transformation: Calculate the count of coaches per country
coaches_per_country = coaches.groupBy("Country").agg(count("Name").alias("CoachCountPerCountry"))

# Join the results together to create a single DataFrame
coaches_transformed = coaches_per_country_per_discipline.join(coaches_per_country, on="Country", how="left")

coaches_transformed.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymics/Transformed-Data/Coaches_Transformed")
coaches_transformed.show()



# COMMAND ----------

coaches.show()
coaches.printSchema()

# COMMAND ----------

#ENTRIESGENDER TRANSFORMATION

from pyspark.sql.functions import col, lit, round, abs

#changing datatypes to integer for female,male and total attriutes.

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
.withColumn("Male",col("Male").cast(IntegerType()))\
.withColumn("Total",col("Total").cast(IntegerType()))

# Transformation: Calculate the percentage of female and male athletes, and the gender difference
entriesgender_transformed = entriesgender.withColumn("FemalePercentage", round((col("Female") / col("Total")) * 100, 2))
entriesgender_transformed = entriesgender_transformed.withColumn("MalePercentage", round((col("Male") / col("Total")) * 100, 2))
entriesgender_transformed = entriesgender_transformed.withColumn("GenderDifference", abs(col("Male") - col("Female")))

# Show the DataFrame
entriesgender_transformed.show(truncate=False)

# Save the transformed dataset as a Parquet file
entriesgender_transformed.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolymics/Transformed-Data/Entriesgender_Transformed")





# COMMAND ----------

entriesgender.printSchema()
entriesgender.show()

# COMMAND ----------

## MEDALS TRANSFORMATION

from pyspark.sql.functions import col, rank, sum, lit, round
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Changing datatypes to integer for rank, gold, silver, bronze, total, and rank by total attributes.
medals = medals.withColumn("rank", col("rank").cast(IntegerType())) \
    .withColumn("gold", col("gold").cast(IntegerType())) \
    .withColumn("silver", col("silver").cast(IntegerType())) \
    .withColumn("bronze", col("bronze").cast(IntegerType())) \
    .withColumn("total", col("total").cast(IntegerType())) \
    .withColumn("rank by total", col("rank by total").cast(IntegerType()))

# Transformation: Calculate the percentage of gold, silver, and bronze medals for each team
medals_transformed = medals.withColumn("GoldPercentage", round((col("gold") / col("total")) * 100, 2))
medals_transformed = medals_transformed.withColumn("SilverPercentage", round((col("silver") / col("total")) * 100, 2))
medals_transformed = medals_transformed.withColumn("BronzePercentage", round((col("bronze") / col("total")) * 100, 2))

# Transformation: Add a "MedalType" column based on the medal count
medals_transformed = medals_transformed.withColumn("MedalType", 
    (col("gold") > 0).cast("int") + (col("silver") > 0).cast("int") + (col("bronze") > 0).cast("int")
)

# Transformation: Add a "HasGold" column to identify teams with gold medals
medals_transformed = medals_transformed.withColumn("HasGold", (col("gold") > 0).cast("int"))

# Save the transformed dataset as a Parquet file
medals_transformed.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolympics/Transformed-Data/Medals_Transformed")

# Show the transformed dataset
medals_transformed.show()









# COMMAND ----------

medals.printSchema()
medals.show()

# COMMAND ----------

# TEAMS TRANSFORMATION

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, lit


# Transformation: Calculate the number of events each team participates in
events_per_team = teams.groupBy("TeamName").agg(count("Discipline").alias("EventsParticipated"))

# Transformation: Calculate the total number of male and female participants for each team
male_participants = teams.filter(col("Event").contains("Men")).groupBy("TeamName").agg(count("Event").alias("MaleParticipants"))
female_participants = teams.filter(col("Event").contains("Women")).groupBy("TeamName").agg(count("Event").alias("FemaleParticipants"))

# Transformation: Calculate the total number of participants for each team
total_participants = teams.groupBy("TeamName").agg(count("Event").alias("TotalParticipants"))

# Transformation: Calculate the total number of events and participants for each team
team_transformed = events_per_team.join(male_participants, on="TeamName", how="left") \
    .join(female_participants, on="TeamName", how="left") \
    .join(total_participants, on="TeamName", how="left")

# Transformation: Calculate the percentage of male and female participants for each team
team_transformed = team_transformed.withColumn("MalePercentage", (col("MaleParticipants") / col("TotalParticipants")) * 100)
team_transformed = team_transformed.withColumn("FemalePercentage", (col("FemaleParticipants") / col("TotalParticipants")) * 100)

# Transformation: Calculate the total number of unique disciplines for each team
unique_disciplines_per_team = teams.groupBy("TeamName").agg(countDistinct("Discipline").alias("UniqueDisciplines"))

# Transformation: Calculate the total number of events and participants for each team
team_transformed = team_transformed.join(unique_disciplines_per_team, on="TeamName", how="left")

# Transformation: Add a column indicating if the team participated in Mixed events
team_transformed = team_transformed.withColumn("ParticipatedInMixed", when(col("EventsParticipated") >= 1, lit("Yes")).otherwise(lit("No")))

# Show the transformed dataset
team_transformed.show()
team_transformed.printSchema()

# Save the transformed dataset as a Parquet file

team_transformed.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolymics/Transformed-Data/team_transformed")

# COMMAND ----------

teams.printSchema()
teams.show()

# COMMAND ----------


