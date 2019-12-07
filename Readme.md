** SOCCER ANALYSIS - TEAM AND PLAYER LEVEL**

** TECHNOLOGIES USED  **
1. Scrapy
2. Mongo
3. Spark
4. Plotly

** FILES INFORMATION **

scrapy.py -  crawls the data from website sofifa.com

```
scrapy crawl spidy
```

Home_away_adv - gives the performance of 2 teams at home and away and player composition of two teams 

team_attack_defensive - gives the attacking and defensive work rate. Can choose any teams - for eg. input 1 -> "Arsenal" input 2-> "Machester United"

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 team_attack_defensive.py 'Real Madrid' 'FC Barcelona'
```

outside_inside_box - gives the goals scored from various playes for each league

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 outside_inside_box.py
```

wage_py - Machine Learing model prediction for a new player who could be replacement of current aging player

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 wage_calculator.py
```

aggresiveness_in_teams - aggressiveness in teams in each league

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 aggressiveness_in_teams.py
```

average_goals_scored - average goals scored by each league over the years

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 average_goals_scored.py
```

correlation_features - relating the players features like pace, stamina, agility

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 correlation_features.py
```
player_replacement - suggests a replacement a given player for a particular position
Can provide any player - "L. Suárez" "Sergio Ramos"

```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 player_replacement.py 'B Matuidi'
```

** CAN RUN ALL THE PROGRAMS ON THE SPARK INSTALLED MACHINE **



