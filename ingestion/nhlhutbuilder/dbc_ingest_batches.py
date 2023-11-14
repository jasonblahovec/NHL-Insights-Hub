# Databricks notebook source
pip install bs4

# COMMAND ----------

# """ Imports"""
import sys
import urllib.request
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import datetime as dt
from pyspark.sql.functions import current_timestamp
import pyspark.sql.functions as F

# COMMAND ----------

class nhl_hut_builder_ingestor():
    def __init__(self, gameyear = 'NHL20', destination_table = '/FileStore/nhl/nhlhutbuilder'):
        self.gameyear = gameyear
        self.destination_table = destination_table

        self.failed_ingestions = []

    def ingest_range_to_table(self, ingest_range = [2046,3046]):
        self.cards = []
        for i in ingest_range:
            try:
                card = self.get_hut_card_by_id(self.gameyear, i)
                self.cards = self.cards + [card]
            except:
                self.failed_ingestions = self.failed_ingestions + [i]

    def write_ingested_records(self):
        self.df_out = spark.createDataFrame(pd.DataFrame(self.cards))
        self.df_out \
            .withColumn("gameyear", F.lit(self.gameyear)) \
            .write.format("parquet").mode("append") \
            .save(self.destination_table)

    @staticmethod
    def get_hut_card_by_id(gameyear, id):
        pass

class nhl_hut_goaltender_ingestor(nhl_hut_builder_ingestor):
    @staticmethod
    def get_hut_card_by_id(gameyear, id):
        def append_goalie_header_attrs(result_dict):
            result_dict['player_full_name'] = str(player_header[0]) \
                .replace('<div id="player_header">', '')\
                .replace('</div>', '')
            result_dict['player_first_name'] = str(player_header[0]) \
                .replace('<div id="player_header">', '') \
                .replace('</div>', '').split(' ')[0]
            result_dict['player_last_name'] = ' '.join(
                str(player_header[0]) \
                .replace('<div id="player_header">', '') \
                    .replace('</div>', '').split(' ')[1:])
            return result_dict

        def append_bio_table_attrs(result_dict):
            titles = re.findall('<td class="bio_title">.*?</td>', str(bio_table))
            results = re.findall('<td class="bio_result">.*?</td>', str(bio_table))
            player_bio_headers = [x.replace('<td class="bio_title">', '') \
                .replace('</td>', '').lower() for x in titles]
            player_bio_values = [x.replace('<td class="bio_result">', '') \
                .replace('</td>', '').lower() for x in results]
            for i in range(len(player_bio_headers)):
                result_dict[player_bio_headers[i]] = player_bio_values[i]
            return result_dict

        def append_player_stats_attrs(result_dict):
            player_attrs = [x.replace('<tr><td>','').replace('</td><td class="stat">','|') 
                            .replace('</td></tr>','') \
                            .split('|') for x in re.findall( \
                                '<tr><td>.*?</td><td class="stat">.*?</td></tr>' \
                                    , str(player_stats))]

            for attr in player_attrs:
                result_dict[attr[0]] = attr[1]
            return result_dict

        """
        For the input id, load the player card
        """
        huturl = f"https://nhlhutbuilder.com/{gameyear}/goalie-stats.php?id={id}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        req = urllib.request.Request(url=huturl, headers=headers)
        page = urllib.request.urlopen(req).read()
        soup = bs(page, 'html')

        """
        Scraping from three areas of the card: 
            player_header contains the name of the player
            bio_table includes player type, card_type, and overall, among other attrs
            player_stats include all available attributes contributing to the player's overall 
        """
        player_header = soup.body.findAll('div', attrs={'id': 'player_header'})
        bio_table = soup.body.findAll('table', attrs={'id': 'player_bio_table'})
        player_stats = soup.body.findAll('div', attrs={'id': 'player_bio_stats'})

        result_dict = {}
        result_dict['player_id'] = id
        result_dict = append_goalie_header_attrs(result_dict)
        result_dict = append_bio_table_attrs(result_dict)
        result_dict = append_player_stats_attrs(result_dict)

        return result_dict

class nhl_hut_skater_ingestor(nhl_hut_builder_ingestor):
    @staticmethod
    def get_hut_card_by_id(gameyear, id):
        def append_player_header_attrs(result_dict):
            result_dict['player_full_name'] = str(player_header[0]) \
                .replace('<div id="player_header">', '').replace(
                '</div>', '')
            result_dict['player_first_name'] = \
            str(player_header[0]).replace('<div id="player_header">', '') \
                .replace('</div>', '').split(' ')[0]
            result_dict['player_last_name'] = ' '.join(
                str(player_header[0]).replace('<div id="player_header">', '') \
                    .replace('</div>', '').split(' ')[1:])
            return result_dict

        def append_bio_table_attrs(result_dict):
            titles = re.findall('<td class="bio_title">.*?</td>', str(bio_table))
            results = re.findall('<td class="bio_result">.*?</td>', str(bio_table))
            player_bio_headers = [x.replace('<td class="bio_title">', '') \
                .replace('</td>', '').lower() for x in titles]
            player_bio_values = [x.replace('<td class="bio_result">', '') \
                .replace('</td>', '').lower() for x in results]
            for i in range(len(player_bio_headers)):
                result_dict[player_bio_headers[i]] = player_bio_values[i]
            return result_dict

        def append_player_stats_attrs(result_dict):
            player_attrs = [x.replace('id="', '') \
                .replace('<', '').replace('>', '').split('"') for x in
                    re.findall('id=".*?">.*?<', str(player_stats))]
            for attr in player_attrs:
                result_dict[attr[0]] = attr[1]
            return result_dict

        """
        For the input id, load the player card
        """
        huturl = f"https://nhlhutbuilder.com/{gameyear}/player-stats.php?id={id}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

        req = urllib.request.Request(url=huturl, headers=headers)
        page = urllib.request.urlopen(req).read()
        soup = bs(page, 'html')
        """
        Scraping from three areas of the card: 
            player_header contains the name of the player
            bio_table includes player type, card_type, and overall, among other attrs
            player_stats include all available attributes contributing to the player's overall 
        """
        player_header = soup.body.findAll('div', attrs={'id': 'player_header'}) 
        bio_table = soup.body.findAll('table', attrs={'id': 'player_bio_table'})
        player_stats = soup.body.findAll('tr', attrs={'class': 'stat_row'})

        result_dict = {}
        result_dict['player_id'] = id
        result_dict = append_player_header_attrs(result_dict)
        result_dict = append_bio_table_attrs(result_dict)
        result_dict = append_player_stats_attrs(result_dict)

        return result_dict
    
class nhl23_hut_goaltender_ingestor(nhl_hut_builder_ingestor):
    @staticmethod
    def get_hut_card_by_id(gameyear, id):
        def append_goalie_header_attrs(result_dict):
            result_dict['player_full_name'] = str(player_header[0]) \
                .replace('<div class="player_header">', '') \
                .replace('</div>', '')
            result_dict['player_first_name'] = str(player_header[0]) \
                .replace('<div class="player_header">', '') \
                .replace('</div>', '').split(' ')[0]
            result_dict['player_last_name'] = ' '.join(
                str(player_header[0]) \
                .replace('<div class="player_header">', '') \
                .replace('</div>', '').split(' ')[1:])
            return result_dict

        def append_bio_table_attrs(result_dict):
            titles = re.findall('<td class="bio_title">.*?</td>', str(bio_table))
            results = re.findall('<td class="bio_result">.*?</td>', str(bio_table))
            player_bio_headers = [x.replace('<td class="bio_title">', '') \
                            .replace('</td>', '').lower() for x in titles]
            player_bio_values = [x.replace('<td class="bio_result">', '') \
                            .replace('</td>', '').lower() for x in results]
            for i in range(len(player_bio_headers)):
                result_dict[player_bio_headers[i]] = player_bio_values[i]
            return result_dict

        def append_player_stats_attrs(result_dict):
            player_attrs = [x.replace('<tr><td>','') \
                        .replace('</td><td class="stat">','|') \
                        .replace('</td></tr>','').split('|') \
                        for x in re.findall('<tr><td>.*?</td><td class="stat">.*?</td></tr>', str(player_stats))]


            for attr in player_attrs:
                result_dict[attr[0]] = attr[1]
            return result_dict

        """
        For the input id, load the player card
        """
        huturl = f"https://nhlhutbuilder.com/{gameyear}/goalie-stats.php?id={id}"
        #NHL 23 URL does not specify year as of 12.14.22
        # huturl = huturl.replace("/NHL23","")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        req = urllib.request.Request(url=huturl, headers=headers)
        page = urllib.request.urlopen(req).read()
        soup = bs(page, 'html')

        """
        Scraping from three areas of the card: 
            player_header contains the name of the player
            bio_table includes player type, card_type, and overall, among other attrs
            player_stats include all available attributes contributing to the player's overall 
        """
        player_header = soup.body.findAll('div', attrs={'class': 'player_header'})
        bio_table = soup.body.findAll('table', attrs={'id': 'player_bio_table'})
        player_stats = soup.body.findAll('div', attrs={'id': 'player_bio_stats'})

        result_dict = {}
        result_dict['player_id'] = id
        result_dict = append_goalie_header_attrs(result_dict)
        result_dict = append_bio_table_attrs(result_dict)
        result_dict = append_player_stats_attrs(result_dict)

        return result_dict

class nhl23_hut_skater_ingestor(nhl_hut_builder_ingestor):
    @staticmethod
    def get_hut_card_by_id(gameyear, id):
        def append_player_header_attrs(result_dict):
            result_dict['player_full_name'] = str(player_header[0]) \
                .replace('<div class="player_header">', '').replace(
                '</div>', '')
            result_dict['player_first_name'] = \
            str(player_header[0]).replace('<div class="player_header">', '') \
                .replace('</div>', '').split(' ')[0]
            result_dict['player_last_name'] = ' '.join(
                str(player_header[0]).replace('<div class="player_header">', '') \
                    .replace('</div>', '').split(' ')[1:])
            return result_dict

        def append_bio_table_attrs(result_dict):
            titles = re.findall('<td class="bio_title">.*?</td>', str(bio_table))
            results = re.findall('<td class="bio_result">.*?</td>', str(bio_table))
            player_bio_headers = [x.replace('<td class="bio_title">', '').replace('</td>', '').lower() for x in titles]
            player_bio_values = [x.replace('<td class="bio_result">', '').replace('</td>', '').lower() for x in results]
            for i in range(len(player_bio_headers)):
                result_dict[player_bio_headers[i]] = player_bio_values[i]
            return result_dict

        def append_player_stats_attrs(result_dict):
            player_attrs = [x.replace('id="', '').replace('<', '').replace('>', '').split('"') for x in
                            re.findall('id=".*?">.*?<', str(player_stats))]
            for attr in player_attrs:
                result_dict[attr[0]] = attr[1]
            return result_dict

        """
        For the input id, load the player card
        """
        huturl = f"https://nhlhutbuilder.com/{gameyear}/player-stats.php?id={id}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

        #NHL 23 URL does not specify year as of 12.14.22
        # huturl = huturl.replace("/NHL23","")
        req = urllib.request.Request(url=huturl, headers=headers)
        page = urllib.request.urlopen(req).read()
        soup = bs(page, 'html')
        """
        Scraping from three areas of the card: 
            player_header contains the name of the player
            bio_table includes player type, card_type, and overall, among other attrs
            player_stats include all available attributes contributing to the player's overall 
        """
        player_header = soup.body.findAll('div', attrs={'class': 'player_header'}) #class is id for NHL20-22
        bio_table = soup.body.findAll('table', attrs={'id': 'player_bio_table'})
        player_stats = soup.body.findAll('tr', attrs={'class': 'stat_row'})

        result_dict = {}
        result_dict['player_id'] = id
        result_dict = append_player_header_attrs(result_dict)
        result_dict = append_bio_table_attrs(result_dict)
        result_dict = append_player_stats_attrs(result_dict)

        return result_dict


# COMMAND ----------

"""
utils
"""
def extract_batches(lst, batch_size, n_batches):
    batches = []
    for i in range(n_batches):
        start = i * batch_size
        end = (i + 1) * batch_size
        batch = lst[start:end]
        batches.append(batch)
    return batches

# COMMAND ----------

if __name__ == "__main__":

    """
    This script is intended for use with Databricks Community Edition.
    It appends a subset of "cards" scraped from a provided range of IDs
    (lines 323-335) to a destination table (lines 342,345).

    Due to constraints with the Community version, the scraping must be done 
    in batches - the size and number of batches are controlled in lines 341/2.

    Ingest across NHL20-23 (line 343) and the entire ranges, pointed at a single 
    table to collect the entire NHLHUTBUILDER history.
    """

    skater_ids = {
            'NHL20': range(2045,8900)
            ,'NHL21': list(range(88811, 99999)) + list(range(8881, 8895))
            ,'NHL22': range(1010,4655)
            ,'NHL23': range(1000,5000)
        }

    goaltender_ids = {
                'NHL20': range(1000,3000)
                ,'NHL21': range(2000,4200)
                ,'NHL22': range(1000,2000)
                ,'NHL23': range(1000,2500)
            }

    gameyear = list(skater_ids.keys())[3]
    batch_size = 1000
    n_batches = 4

    run_skater = True
    skater_destination_table = '/FileStore/nhl/ea/skaters'

    run_goaltender = False
    goaltender_destination_table = '/FileStore/nhl/ea/goaltenders'

    if run_skater:
        if gameyear == 'NHL23':
            skater_ingestor = nhl23_hut_skater_ingestor(gameyear = gameyear, destination_table = skater_destination_table)
        else:
            skater_ingestor = nhl_hut_skater_ingestor(gameyear = gameyear, destination_table = skater_destination_table)

        skater_batches = extract_batches(skater_ids[gameyear], batch_size, n_batches)
        for i,skater_batch in enumerate(skater_batches):
            print(f"Batch {i} begin {dt.datetime.now()}")
            skater_ingestor.ingest_range_to_table(ingest_range = skater_batch)
            skater_ingestor.write_ingested_records()

    if run_goaltender:
        if gameyear == 'NHL23':
            goaltender_ingestor = nhl23_hut_goaltender_ingestor(gameyear = gameyear, destination_table = goaltender_destination_table)
        else:
            goaltender_ingestor = nhl_hut_goaltender_ingestor(gameyear = gameyear, destination_table = goaltender_destination_table)

        goaltender_batches = extract_batches(goaltender_ids[gameyear], batch_size, n_batches)
        for goaltender_batch in goaltender_batches:
            goaltender_ingestor.ingest_range_to_table(ingest_range = goaltender_batch)
            goaltender_ingestor.write_ingested_records()


# COMMAND ----------

spark.read.format("parquet").load('/FileStore/nhl/ea/skaters').display()
spark.read.format("parquet").load('/FileStore/nhl/ea/goaltenders').display()
