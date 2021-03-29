Tasks: 

    - Count number of awards for each player 
	- Count number of goals (Scoring.stint) 
	- Find in which teams played each player 
	- Limit top 10 players by goals and save in text format (and few other formats of your choice, avro, parquet...) 
	- Write it in 3 methods: RDD, Datasets API and SQL query


example of a launch: 

```bash
/opt/bitnami/spark/bin/spark-submit --master spark://spark:7077 /tmp/HockeyPy2/spark_tasks/spark_hockey.py /tmp/HockeyPy2/resources/
```


the example of the command output can be found in `output_example.txt`