import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from itertools import islice
from pyspark.sql.functions import *
import os


class SparkHockey:
    APP_NAME = "PythonHockeyApp"

    # MASTER_SETTING = "local[2]"

    def __init__(self):
        self.spark_conf = (SparkConf()
                           .setAppName(SparkHockey.APP_NAME))
        # .setMaster(MASTER_SETTING))
        # self.spark_context = SparkContext(conf=self.spark_conf)
        self.spark_context = SparkContext.getOrCreate(conf=self.spark_conf)
        self.spark_session = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()

        self.resources_folder = sys.argv[1].rstrip("/")
        self.awards_file_name = os.path.join(self.resources_folder, "AwardsPlayers.csv")
        self.scoring_file_name = os.path.join(self.resources_folder, "Scoring.csv")
        self.teams_file_name = os.path.join(self.resources_folder, "Teams.csv")

    def get_sql_session(self):
        return self.spark_session;

    def get_rdd_from_csv_file(self, csv_file_name: str, with_header: bool = False):
        rdd = self.spark_context.textFile(name=csv_file_name)
        if not with_header:
            rdd = rdd.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
        return rdd

    def get_data_frame_from_csv_file(self, csv_file_name: str,
                                     header_option: str = "true", infer_schema_option: str = "true"):
        data_frame = (self.spark_session.read.format("csv")
                      .option("header", header_option)
                      .option("inferSchema", infer_schema_option)
                      .load(csv_file_name))
        return data_frame

    @staticmethod
    def save_rdd_into_file(saved_rdd, header: list = None,
                           saved_format: str = "csv", folder_name: str = "rdd_folder"):
        if saved_format == "txt":
            saved_rdd.saveAsTextFile(folder_name)

        if saved_format == "csv":
            def to_csv_line(data):
                return ','.join(str(d) for d in data)

            saved_rdd.map(to_csv_line).saveAsTextFile(folder_name)

        if saved_format == "parquet":
            saved_df = saved_rdd.toDF(header)
            saved_df.write.parquet(folder_name)
        return saved_rdd

    @staticmethod
    def save_df_into_file(data_frame, saved_format: str = "csv", folder_name: str = "df_folder"):
        if saved_format == "csv":
            data_frame.write.format('com.databricks.spark.csv').save(folder_name)

        if saved_format == "parquet":
            data_frame.write.parquet(folder_name)


class Awards(SparkHockey):

    def get_awards_rdd(self, is_print: bool = False):
        # read content of the file without header
        awards_rdd = self.get_rdd_from_csv_file(self.awards_file_name, with_header=False)

        # RDD. Count number of awards for each player
        awards_by_people_rdd = awards_rdd.map(lambda record: (record.split(",")[0], 1)) \
            .reduceByKey(lambda x, y: x + y)

        if is_print:
            print("Result: RDD. Count number of awards for each player")
            for record_man_numbers in awards_by_people_rdd.collect():
                print(record_man_numbers)

        return awards_by_people_rdd

    def get_awards_data_frame(self, is_print: bool = False):
        # Datasets API
        awards_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.awards_file_name)

        awards_by_people_df = awards_data_frame.groupBy("playerID").agg(count("playerID").alias("countAwards"))
        if is_print:
            awards_by_people_df.show()
        return awards_by_people_df

    def get_awards_sql(self, is_print: bool = False):
        awards_sql = self.get_data_frame_from_csv_file(csv_file_name=self.awards_file_name)

        awards_sql.createOrReplaceTempView("awards")
        awards_sql_df = self.spark_session.sql("SELECT playerID, count(playerID) as count_awards FROM awards "
                                               "GROUP BY playerID "
                                               "ORDER BY count_awards DESC")
        if is_print:
            awards_sql_df.show()
        return awards_sql_df


class Goals(SparkHockey):

    def get_goals_number_rdd(self, is_print: bool = False):
        # Count number of goals (Scoring.stint)
        score_rdd = self.get_rdd_from_csv_file(self.scoring_file_name, with_header=False)
        only_goals_rdd = score_rdd.map(lambda line: int(line.split(",")[7]) if line.split(",")[7].isnumeric() else 0)
        sum_goals_rdd = only_goals_rdd.sum()

        if is_print:
            print(sum_goals_rdd)

        return sum_goals_rdd

    def get_goals_number_data_frame(self, is_print: bool = False):
        # Count number of goals (Scoring.stint)
        score_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.scoring_file_name)

        total_goals_df = score_data_frame.agg(sum("G"))

        if is_print:
            total_goals_df.show()
        return total_goals_df

    def get_goals_number_sql(self, is_print: bool = False):
        # Count number of goals (Scoring.stint)
        goals_sql = self.get_data_frame_from_csv_file(csv_file_name=self.scoring_file_name)
        goals_sql.createOrReplaceTempView("scoring")
        total_goals = self.spark_session.sql("SELECT SUM(G) as total_goals "
                                             "FROM scoring;")
        if is_print:
            total_goals.show()
        return total_goals


class PlayersTeams(SparkHockey):

    def get_players_teams_rdd(self, is_print: bool = False):
        # Find in which teams played each player
        players_rdd = self.get_rdd_from_csv_file(self.scoring_file_name, with_header=False)
        teams_rdd = self.get_rdd_from_csv_file(self.teams_file_name, with_header=False)
        teams_id_name_rdd = teams_rdd.map(lambda teams_record:
                                          (teams_record.split(",")[2], teams_record.split(",")[18])) \
            .reduceByKey(lambda k1, k2: k1)
        players_teams_rdd = players_rdd.map(lambda record: (record.split(",")[3], record.split(",")[0]))

        team_player_join_rdd = players_teams_rdd.join(teams_id_name_rdd) \
            .map(lambda x: (x[1][0], [x[1][1]])) \
            .reduceByKey(lambda x1, x2: x1 + x2 if x2[0] not in x1 else x1)

        if is_print:
            for player_team in team_player_join_rdd.collect():
                print(player_team)
        return team_player_join_rdd

    def get_players_teams_data_frame(self, is_print: bool = True):
        players_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.scoring_file_name)
        teams_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.teams_file_name)

        teams_data_frame = teams_data_frame.select("tmID", "name").drop_duplicates(["tmID"])
        players_data_frame = players_data_frame.select("playerID", "tmID").distinct()

        team_player_join_rdd = players_data_frame.join(teams_data_frame, on=["tmID"]) \
            .select("playerID", "name").groupBy("playerID").agg(collect_list("name").alias("Teams"))

        if is_print:
            team_player_join_rdd.show(n=46000, truncate=False)

        return team_player_join_rdd

    def get_players_teams_sql(self, is_print: bool = True):
        players_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.scoring_file_name)
        teams_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.teams_file_name)

        teams_data_frame.createOrReplaceTempView("teams")
        teams_id_name_df = self.spark_session.sql("SELECT tmID, MIN(name) as name FROM teams GROUP BY tmID")
        teams_id_name_df.createOrReplaceTempView("teams2")
        players_data_frame.createOrReplaceTempView("score")

        result = self.spark_session.sql(
            "SELECT DISTINCT score.playerID, concat_ws(',', collect_set(teams2.name)) as teams "
            "FROM score LEFT JOIN teams2 "
            "ON score.tmID = teams2.tmID "
            "GROUP BY score.playerID")

        if is_print:
            result.show(n=46000, truncate=False)

        return result


class TopPlayers(SparkHockey):
    # Limit top 10 players by goals and save in text format (and few other formats of your choice, avro, parquet...)
    def get_top_players_by_goals_rdd(self, is_print: bool = True, number_top_players: int = 0, is_saved: bool = False):
        score_rdd = self.get_rdd_from_csv_file(self.scoring_file_name, with_header=False)
        players_goals_rdd = score_rdd.map(
            lambda line: (line.split(",")[0], int(line.split(",")[7]) if line.split(",")[7].isnumeric() else 0))

        players_goals_reduced_rdd = players_goals_rdd.reduceByKey(lambda x1, x2: x1 + x2)
        players_goals_top_rdd = players_goals_reduced_rdd

        if number_top_players:
            players_goals_top_list = players_goals_reduced_rdd.takeOrdered(number_top_players,
                                                                           lambda record: -record[1])
            players_goals_top_rdd = self.spark_context.parallelize(players_goals_top_list)

        if is_print:
            for pl_goals in players_goals_top_rdd.collect():
                print(pl_goals)

        if is_saved:
            self.save_rdd_into_file(saved_rdd=players_goals_top_rdd, saved_format="csv", folder_name="rdd_csv")
            self.save_rdd_into_file(saved_rdd=players_goals_top_rdd, saved_format="txt", folder_name="rdd_txt")
            self.save_rdd_into_file(saved_rdd=players_goals_top_rdd, saved_format="parquet", folder_name="rdd_parquet",
                                    header=["playerID", "totalGoals"])
        return players_goals_top_rdd

    def get_top_players_by_goals_data_frame(self, is_print=True, number_top_players: int = 0, is_saved: bool = False):
        score_data_frame = self.get_data_frame_from_csv_file(self.scoring_file_name)
        top_players_data_frame = score_data_frame.select("playerID", "G").groupBy("playerID") \
            .agg(sum("G").alias("totalGoals")).orderBy(col("totalGoals").desc())

        if number_top_players:
            top_players_data_frame = top_players_data_frame.limit(10)

        if is_print:
            top_players_data_frame.show()

        if is_saved:
            self.save_df_into_file(top_players_data_frame, saved_format="csv", folder_name="df_csv")
            self.save_df_into_file(top_players_data_frame, saved_format="parquet", folder_name="df_parquet")

        return top_players_data_frame

    def get_top_players_by_goals_sql(self, is_print=True, number_top_players: int = 0, is_saved: bool = False):
        score_data_frame = self.get_data_frame_from_csv_file(csv_file_name=self.scoring_file_name)
        score_data_frame.createOrReplaceTempView("Scoring")
        top_players_data_frame = self.spark_session.sql("SELECT playerID, sum(G) as totalGoals "
                                                        "FROM Scoring "
                                                        "GROUP BY playerID "
                                                        "ORDER BY totalGoals DESC "
                                                        "%s" % "LIMIT " + str(
            number_top_players) if number_top_players else "")
        if is_print:
            top_players_data_frame.show()

        if is_saved:
            self.save_df_into_file(top_players_data_frame, saved_format="csv", folder_name="df_sql_csv")
            self.save_df_into_file(top_players_data_frame, saved_format="parquet", folder_name="df_sql_parquet")

        return top_players_data_frame


class Task:

    # - Count number of awards for each player
    # - Count number of goals (Scoring.stint)
    # - Find in which teams played each player
    # - Limit top 10 players by goals and save in text format (and few other formats of your choice, avro, parquet...)
    # - Write it in 3 methods: RDD, Datasets API and SQL query

    @staticmethod
    def get_result_in_rdd(is_print: bool = False, is_saved: bool = False,
                          saved_format="parquet", folder_name="rdd_folder"):
        awards_by_people_rdd = Awards().get_awards_rdd(is_print=False)
        goals_by_people_rdd = TopPlayers().get_top_players_by_goals_rdd(is_print=False, number_top_players=10)
        teams_by_people_rdd = PlayersTeams().get_players_teams_rdd(is_print=False)

        awards_and_goals_and_teams_rdd = goals_by_people_rdd.join(awards_by_people_rdd).join(teams_by_people_rdd)
        if is_print:
            for player_team in awards_and_goals_and_teams_rdd.collect():
                print(player_team)

        if is_saved:
            SparkHockey.save_rdd_into_file(saved_rdd=awards_and_goals_and_teams_rdd,
                                           saved_format=saved_format,
                                           folder_name=folder_name)
        return awards_and_goals_and_teams_rdd

    @staticmethod
    def get_result_in_df(is_print: bool = False, is_saved: bool = False, saved_format="parquet", folder_name="df_folder"):
        awards_by_people_df = Awards().get_awards_data_frame(is_print=False)
        goals_by_people_df = TopPlayers().get_top_players_by_goals_data_frame(is_print=False, number_top_players=10)
        teams_by_people_df = PlayersTeams().get_players_teams_data_frame(is_print=False)

        awards_and_goals_and_teams_df = teams_by_people_df.join(goals_by_people_df, on="playerID") \
            .join(awards_by_people_df, on="playerID")

        if is_print:
            awards_and_goals_and_teams_df.show(truncate=False)

        if is_saved:
            SparkHockey.save_df_into_file(awards_and_goals_and_teams_df,
                                          saved_format=saved_format, folder_name=folder_name)

        return awards_and_goals_and_teams_df

    @staticmethod
    def get_result_in_sql(is_print: bool = False, is_saved: bool = False,
                          saved_format="parquet", folder_name="sql_folder"):
        awards_by_people_df = Awards().get_awards_sql(is_print=False)
        awards_by_people_df.createOrReplaceTempView("awardsByPeople")

        goals_by_people_df = TopPlayers().get_top_players_by_goals_sql(is_print=False, number_top_players=10)
        goals_by_people_df.createOrReplaceTempView("goalsByPeople")

        teams_by_people_df = PlayersTeams().get_players_teams_sql(is_print=False)
        teams_by_people_df.createOrReplaceTempView("teamsByPeople")

        sql_session = PlayersTeams().get_sql_session()

        awards_and_goals_and_teams_sql = sql_session.sql(
            "SELECT teamsByPeople.playerID, teamsByPeople.teams, goalsByPeople.totalGoals, awardsByPeople.count_awards "
            "FROM teamsByPeople INNER JOIN goalsByPeople "
            "ON teamsByPeople.playerID = goalsByPeople.playerID "
            "INNER JOIN awardsByPeople " 
            "ON teamsByPeople.playerID = awardsByPeople.playerID")

        if is_print:
            awards_and_goals_and_teams_sql.show(truncate=False)

        if is_saved:
            SparkHockey.save_df_into_file(awards_and_goals_and_teams_sql,
                                          saved_format=saved_format, folder_name=folder_name)


if __name__ == "__main__":
    # awards = Awards()
    # awards.get_awards_rdd(is_print=True)
    # awards.get_awards_data_frame(is_print=True)
    # awards.get_awards_sql(is_print=True)
    #
    # goals = Goals()
    # goals.get_goals_number_rdd(is_print=True)
    # goals.get_goals_number_data_frame(is_print=True)
    # goals.get_goals_number_sql(is_print=True)
    #
    # players_teams = PlayersTeams()
    # players_teams.get_players_teams_rdd(is_print=True)
    # players_teams.get_players_teams_data_frame(is_print=True)
    # players_teams.get_players_teams_sql(is_print=True)
    #
    # top_players = TopPlayers()
    # top_players.get_top_players_by_goals_rdd(is_print=True)
    # top_players.get_top_players_by_goals_data_frame(is_print=True)
    # top_players.get_top_players_by_goals_sql(is_print=True)

    Task.get_result_in_rdd(is_print=True, is_saved=True)
    Task.get_result_in_df(is_print=True, is_saved=True)
    Task.get_result_in_sql(is_print=True, is_saved=True)
