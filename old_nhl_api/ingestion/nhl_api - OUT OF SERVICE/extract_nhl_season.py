import requests
import pandas as pd
import argparse
import pyspark

class ingest_nhl_teams():
    """
    ingest_nhl_teams creates self.teams_df upon instantiation. This 
    pandas df captures NHL tem level information as found at 
    https://statsapi.web.nhl.com/api/v1/teams.
    """
    def get_teams(self):
        raw_teams = requests.get(f"https://statsapi.web.nhl.com/api/v1/teams").json()['teams']
        teamsdict = {}
        for team in raw_teams:
            teamdict = {}
            teamdict['team_id'] = team['id']
            teamdict['name'] = team['name']
            teamdict['venue'] = team['venue']['name']
            teamdict['city'] = team['venue']['city']
            teamdict['abbreviation'] = team['abbreviation']
            teamdict['teamName'] = team['teamName']
            teamdict['location'] = team['locationName']
            teamdict['firstSeason'] = team['firstYearOfPlay']
            teamdict['division_id'] = team['division']['id']
            teamdict['division_name'] = team['division']['name']
            teamdict['conference_id'] = team['conference']['id']
            teamdict['conference_name'] = team['conference']['name']
            teamdict['shortName'] = team['shortName']
            teamdict['franchiseId'] = team['franchiseId']
            teamdict['firstYearOfPlay'] = team['firstYearOfPlay']
            teamsdict[teamdict['name']] = teamdict
        self.teamsdict = teamsdict
        self.teams_df = pd.DataFrame.from_dict(teamsdict, orient='index')

    def __init__(self):
        self.get_teams()

class get_nhl_game_data():
    """
    get_nhl_game_data extracts information from the NHL API
    relevant to a single game using the url 
    https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live.
    """
    def __init__(self, game_id):
        self.game_id = game_id
        self.game_feed = requests.get(f"https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live").json()
        self.hasData = True
        try:
            if self.game_feed['messageNumber']==2:
                self.hasData = False
        except:
            pass
        if self.hasData:
            self.game_info = self._get_game_info()
            self.isComplete = False
            if self.game_state =='Final':
                self.isComplete = True
                self.df_players = self._get_game_players()
                self.append_arena_info()
                self.append_boxscore_teamstats()
                self.df_officials = self.get_officials()
                self.df_playerstats = self.get_boxscore_playerstats()
                self.df_plays = self.get_game_plays()
                self.append_team_period_stats()
    @staticmethod
    def extract_to_dict(_destination_dict,_destination_key,_source,_source_reference):
        try:
            _v = _source
            for _level in _source_reference:
                _v = _v[_level]
            _destination_dict[_destination_key] = _v
        except KeyError:
            _destination_dict[_destination_key] = None

    def _get_game_info(self):
        game_info = {}
        self.extract_to_dict(game_info,'game_pk',self.game_feed,['gameData','game','pk'])
        self.extract_to_dict(game_info,'season',self.game_feed,['gameData','game','season'])
        self.extract_to_dict(game_info,'game_type',self.game_feed,['gameData','game','type'])
        self.extract_to_dict(game_info,'game_start',self.game_feed,['gameData','datetime','dateTime'])
        self.extract_to_dict(game_info,'game_end',self.game_feed,['gameData','datetime','endDateTime'])
        self.extract_to_dict(game_info,'game_state',self.game_feed,['gameData','status','detailedState'])
        self.game_state = game_info['game_state']
        self.extract_to_dict(game_info,'game_time_tbd',self.game_feed,['gameData','status','startTimeTBD'])
        self.extract_to_dict(game_info,'team_away_id',self.game_feed,['gameData','teams','away','id'])
        self.extract_to_dict(game_info,'team_away_name',self.game_feed,['gameData','teams','away','name'])
        self.extract_to_dict(game_info,'team_away_shortname',self.game_feed,['gameData','teams','away','shortName'])
        self.extract_to_dict(game_info,'team_home_id',self.game_feed,['gameData','teams','home','id'])
        self.extract_to_dict(game_info,'team_home_name',self.game_feed,['gameData','teams','home','name'])
        self.extract_to_dict(game_info,'team_home_shortname',self.game_feed,['gameData','teams','home','shortName'])
        self.extract_to_dict(game_info,'winning_goaltender_id',self.game_feed,['liveData','decisions','winner','id'])
        self.extract_to_dict(game_info,'losing_goaltender_id',self.game_feed,['liveData','decisions','loser','id'])
        self.extract_to_dict(game_info,'first_star_id',self.game_feed,['liveData','decisions','firstStar','id'])
        self.extract_to_dict(game_info,'second_star_id',self.game_feed,['liveData','decisions','secondStar','id'])
        self.extract_to_dict(game_info,'third_star_id',self.game_feed,['liveData','decisions','thirdStar','id'])
        self.extract_to_dict(game_info,'winning_goaltender_name',self.game_feed,['liveData','decisions','winner','fullName'])
        self.extract_to_dict(game_info,'losing_goaltender_name',self.game_feed,['liveData','decisions','loser','fullName'])
        self.extract_to_dict(game_info,'first_star_name',self.game_feed,['liveData','decisions','firstStar','fullName'])
        self.extract_to_dict(game_info,'second_star_name',self.game_feed,['liveData','decisions','secondStar','fullName'])
        self.extract_to_dict(game_info,'third_star_name',self.game_feed,['liveData','decisions','thirdStar','fullName'])
        return game_info

    def _get_game_players(self):
        game_players = {}
        for player in self.game_feed['gameData']['players'].keys():
            playerdict = {}
            row = self.game_feed['gameData']['players'][player]
            playerdict['player_id'] = [player]
            self.extract_to_dict(playerdict,'player_id_num',row,['id'])
            self.extract_to_dict(playerdict,'fullName',row,['fullName'])
            self.extract_to_dict(playerdict,'firstName',row,['firstName'])
            self.extract_to_dict(playerdict,'lastName',row,['lastName'])
            self.extract_to_dict(playerdict,'primaryNumber',row,['primaryNumber'])
            self.extract_to_dict(playerdict,'birthDate',row,['birthDate'])
            self.extract_to_dict(playerdict,'currentAge',row,['currentAge'])
            self.extract_to_dict(playerdict,'birthCity',row,['birthCity'])
            self.extract_to_dict(playerdict,'birthStateProvince',row,['birthStateProvince'])
            self.extract_to_dict(playerdict,'birthCountry',row,['birthCountry'])
            self.extract_to_dict(playerdict,'nationality',row,['nationality'])
            self.extract_to_dict(playerdict,'height',row,['height'])
            self.extract_to_dict(playerdict,'weight',row,['weight'])
            self.extract_to_dict(playerdict,'active',row,['active'])
            self.extract_to_dict(playerdict,'captain',row,['captain'])
            self.extract_to_dict(playerdict,'alternateCaptain',row,['ialternateCaptaind'])
            self.extract_to_dict(playerdict,'rookie',row,['rookie'])
            self.extract_to_dict(playerdict,'shootsCatches',row,['shootsCatches'])
            self.extract_to_dict(playerdict,'rosterStatus',row,['rosterStatus'])
            self.extract_to_dict(playerdict,'currentTeamId',row,['currentTeam','id'])
            self.extract_to_dict(playerdict,'currentTeamName',row,['currentTeam','name'])
            self.extract_to_dict(playerdict,'currentTeamCode',row,['currentTeam','triCode'])
            self.extract_to_dict(playerdict,'primaryPositionCode',row,['primaryPosition','code'])
            self.extract_to_dict(playerdict,'primaryPositionName',row,['primaryPosition','name'])
            self.extract_to_dict(playerdict,'primaryPositionType',row,['primaryPosition','type'])
            self.extract_to_dict(playerdict,'primaryPositionAbbr',row,['primaryPosition','abbreviation'])
            game_players[str(self.game_id) + str(player)] = playerdict
        df_players = pd.DataFrame.from_dict(game_players, orient='index')
        return df_players

    def append_arena_info(self):
        self.extract_to_dict(self.game_info,'arena_id',self.game_feed,['gameData','venue','id'])
        self.extract_to_dict(self.game_info,'name',self.game_feed,['gameData','venue','name'])

    def get_officials(self):
        officialsdict = {}
        for official in self.game_feed['liveData']['boxscore']['officials']:
            temp_dict = {'game_id': self.game_id}
            self.extract_to_dict(temp_dict,'official_id',official,['official','id'])
            self.extract_to_dict(temp_dict,'official_name',official,['official','fullName'])
            self.extract_to_dict(temp_dict,'official_type',official,['official','officialType'])
            officialsdict[str(self.game_id) + str(temp_dict['official_id'])] = temp_dict
        df_officials = pd.DataFrame.from_dict(officialsdict, orient='index')
        return df_officials

    def append_boxscore_teamstats(self):

        for teamtype in ['away', 'home']:
            self.extract_to_dict(self.game_info,f'{teamtype}_goals',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','goals'])
            self.extract_to_dict(self.game_info,f'{teamtype}_pim',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','pim'])
            self.extract_to_dict(self.game_info,f'{teamtype}_shots',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','shots'])
            self.extract_to_dict(self.game_info,f'{teamtype}_pp_pct',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','powerPlayPercentage'])
            self.extract_to_dict(self.game_info,f'{teamtype}_ppgoal',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','powerPlayGoals'])
            self.extract_to_dict(self.game_info,f'{teamtype}_ppcount',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats', 'powerPlayOpportunities'])
            self.extract_to_dict(self.game_info,f'{teamtype}_fowinpct',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','faceOffWinPercentage'])
            self.extract_to_dict(self.game_info,f'{teamtype}_blocks',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','blocked'])
            self.extract_to_dict(self.game_info,f'{teamtype}_takeaways',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','takeaways'])
            self.extract_to_dict(self.game_info,f'{teamtype}_giveaways',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','giveaways'])
            self.extract_to_dict(self.game_info,f'{teamtype}_hits',self.game_feed, \
                ['liveData','boxscore','teams',teamtype,'teamStats','teamSkaterStats','hits'])

    def get_boxscore_playerstats(self):
        game_players = {}
        head_coaches = {}
        for teamtype in ['away', 'home']:
            
            hcs = [x['person']['fullName'] for x in  \
                self.game_feed['liveData']['boxscore']['teams'][teamtype]['coaches']  \
                    if x['position']['code']== 'HC']
            if len(hcs)==1:
                head_coaches[teamtype] = hcs[0]
            elif len(hcs)==0:
                head_coaches[teamtype] = None
            else:
                raise ValueError(f'multiple head coaches in game {self.game_id}/{teamtype}.')

            players = self.game_feed['liveData']['boxscore']['teams'][teamtype]['players']
            for player in players:
                playerdict = {}
                player_person = players[player]['person']
                player_position = players[player]['position']
                player_stats = players[player]['stats']
                playerdict['player_teamtype'] = teamtype
                try:
                    player_jersey_number = players[player]['jerseyNumber']
                except KeyError:
                    player_jersey_number = None
                
                try:
                    skaterStats = player_stats['skaterStats']
                except KeyError:
                    skaterStats = {}
                
                try:
                    goalieStats = player_stats['goalieStats']
                except KeyError:
                    goalieStats = {}
                
                self.extract_to_dict(playerdict,'player_id',player_person,['id'])
                self.extract_to_dict(playerdict,'player_fullname',player_person,['fullName'])
                self.extract_to_dict(playerdict,'player_handed',player_person,['shootsCatches'])
                self.extract_to_dict(playerdict,'player_rosterStatus',player_person,['rosterStatus'])
                self.extract_to_dict(playerdict,'player_jersey_number',players[player],['jerseyNumber'])
                self.extract_to_dict(playerdict,'player_poscode',player_position,['code'])
                self.extract_to_dict(playerdict,'player_position',player_position,['name'])
                self.extract_to_dict(playerdict,'player_postype',player_position,['type'])
                self.extract_to_dict(playerdict,'player_posabbrev',player_position,['abbreviation'])                
                self.extract_to_dict(playerdict,'assists',skaterStats,['assists'])
                self.extract_to_dict(playerdict,'goals',skaterStats,['goals'])
                self.extract_to_dict(playerdict,'shots',skaterStats,['shots'])
                self.extract_to_dict(playerdict,'hits',skaterStats,['hits'])
                self.extract_to_dict(playerdict,'powerPlayGoals',skaterStats,['powerPlayGoals'])
                self.extract_to_dict(playerdict,'powerPlayAssists',skaterStats,['powerPlayAssists'])
                self.extract_to_dict(playerdict,'penaltyMinutes',skaterStats,['penaltyMinutes'])
                self.extract_to_dict(playerdict,'faceOffPct',skaterStats,['faceOffPct'])
                self.extract_to_dict(playerdict,'faceOffWins',skaterStats,['faceOffWins'])
                self.extract_to_dict(playerdict,'faceoffTaken',skaterStats,['faceoffTaken'])
                self.extract_to_dict(playerdict,'takeaways',skaterStats,['takeaways'])
                self.extract_to_dict(playerdict,'giveaways',skaterStats,['giveaways'])
                self.extract_to_dict(playerdict,'shortHandedGoals',skaterStats,['shortHandedGoals'])
                self.extract_to_dict(playerdict,'shortHandedAssists',skaterStats,['shortHandedAssists'])
                self.extract_to_dict(playerdict,'blocked',skaterStats,['blocked'])
                self.extract_to_dict(playerdict,'plusMinus',skaterStats,['plusMinus'])
                self.extract_to_dict(playerdict,'evenTimeOnIce',skaterStats,['evenTimeOnIce'])
                self.extract_to_dict(playerdict,'powerPlayTimeOnIce',skaterStats,['powerPlayTimeOnIce'])
                self.extract_to_dict(playerdict,'shortHandedTimeOnIce',skaterStats,['shortHandedTimeOnIce'])
                self.extract_to_dict(playerdict,'shortHandedGoals',skaterStats,['shortHandedGoals'])
                self.extract_to_dict(playerdict,'shortHandedGoals',skaterStats,['shortHandedGoals'])
                self.extract_to_dict(playerdict,'shortHandedGoals',skaterStats,['shortHandedGoals'])
                self.extract_to_dict(playerdict,'shortHandedGoals',skaterStats,['shortHandedGoals'])
                self.extract_to_dict(playerdict,'gt_timeOnIce',goalieStats,['timeOnIce'])
                self.extract_to_dict(playerdict,'gt_assists',goalieStats,['assists'])
                self.extract_to_dict(playerdict,'gt_goals',goalieStats,['goals'])
                self.extract_to_dict(playerdict,'gt_pim',goalieStats,['pim'])
                self.extract_to_dict(playerdict,'gt_shots',goalieStats,['shots'])
                self.extract_to_dict(playerdict,'gt_saves',goalieStats,['saves'])
                self.extract_to_dict(playerdict,'gt_powerPlaySaves',goalieStats,['powerPlaySaves'])
                self.extract_to_dict(playerdict,'gt_shortHandedSaves',goalieStats,['shortHandedSaves'])
                self.extract_to_dict(playerdict,'gt_evenSaves',goalieStats,['evenSaves'])
                self.extract_to_dict(playerdict,'gt_shortHandedShotsAgainst',goalieStats,['shortHandedShotsAgainst'])
                self.extract_to_dict(playerdict,'gt_evenShotsAgainst',goalieStats,['evenShotsAgainst'])
                self.extract_to_dict(playerdict,'gt_powerPlayShotsAgainst',goalieStats,['powerPlayShotsAgainst'])
                self.extract_to_dict(playerdict,'gt_decision',goalieStats,['decision'])
                self.extract_to_dict(playerdict,'gt_savePercentage',goalieStats,['savePercentage'])
                self.extract_to_dict(playerdict,'gt_evenStrengthSavePercentage',goalieStats,['evenStrengthSavePercentage'])

                game_players[str(self.game_id) + str(player)] = playerdict
        self.extract_to_dict(self.game_info,'head_coach_away',head_coaches,['away'])
        self.extract_to_dict(self.game_info,'head_coach_home',head_coaches,['home'])
        df_players = pd.DataFrame.from_dict(game_players, orient='index')
        return df_players

    def get_game_plays(self):
        game_plays = self.game_feed['liveData']['plays']['allPlays']
        gameplaydict = {}
        for i in range(0, len(game_plays)):
            playdict = {}
            playdict['game_id'] = self.game_id
            self.extract_to_dict(playdict,'event',game_plays,[i,'result','event'])
            self.extract_to_dict(playdict,'eventCode',game_plays,[i,'result','eventCode'])
            self.extract_to_dict(playdict,'eventTypeId',game_plays,[i,'result','eventTypeId'])
            self.extract_to_dict(playdict,'description',game_plays,[i,'result','description'])
            self.extract_to_dict(playdict,'eventIdx',game_plays,[i,'about','eventIdx'])
            self.extract_to_dict(playdict,'eventId',game_plays,[i,'about','eventId'])
            self.extract_to_dict(playdict,'period',game_plays,[i,'about','period'])
            self.extract_to_dict(playdict,'periodType',game_plays,[i,'about','periodType'])
            self.extract_to_dict(playdict,'ordinalNum',game_plays,[i,'about','ordinalNum'])
            self.extract_to_dict(playdict,'periodTime',game_plays,[i,'about','periodTime'])
            self.extract_to_dict(playdict,'periodTimeRemaining',game_plays,[i,'about','periodTimeRemaining'])
            self.extract_to_dict(playdict,'dateTime',game_plays,[i,'about','dateTime'])
            self.extract_to_dict(playdict,'goals',game_plays,[i,'about','goals'])
            self.extract_to_dict(playdict,'x',game_plays,[i,'coordinates','x'])
            self.extract_to_dict(playdict,'y',game_plays,[i,'coordinates','y'])

            gameplaydict[playdict['game_id'] + '_' + str(i)] = playdict
        df_plays = pd.DataFrame.from_dict(gameplaydict, orient='index')
        return df_plays

    def append_team_period_stats(self):
        periods = self.game_feed['liveData']['linescore']['periods']
        shootout = self.game_feed['liveData']['linescore']['shootoutInfo']
        shootout['periodType'] = 'SHOOTOUT'

        if len([x['periodType'] for x in periods if x['periodType'] =='OVERTIME'])==0:
            periods.append({'periodType': 'OVERTIME', 'num':4}  )

        for period in periods+[shootout]:
            for team_type in ['away','home']:
                if period['periodType'] == 'REGULAR':
                    self.extract_to_dict(self.game_info,f"p{period['num']}_{team_type}_goals",period,[team_type,'goals'])
                    self.extract_to_dict(self.game_info,f"p{period['num']}_{team_type}_shots",period,[team_type,'shotsOnGoal'])
                    self.extract_to_dict(self.game_info,f"p{period['num']}_{team_type}_rink_side",period,[team_type,'rinkSide'])
                elif period['periodType'] == 'OVERTIME':
                    if period['num'] <= 4:
                        self.extract_to_dict(self.game_info,f"ot{period['num']}_{team_type}_goals",period,[team_type,'goals'])
                        self.extract_to_dict(self.game_info,f"ot{period['num']}_{team_type}_shots",period,[team_type,'shotsOnGoal'])
                        self.extract_to_dict(self.game_info,f"ot{period['num']}_{team_type}_rink_side",period,[team_type,'rinkSide'])
                    else:
                        """
                        Multi-OT for playoff games currently not supported.
                        """
                        pass
                elif period['periodType'] == 'SHOOTOUT':
                    self.extract_to_dict(self.game_info,f"so_attempts_{team_type}",period,[team_type,'attempts'])
                    self.extract_to_dict(self.game_info,f"so_goals_{team_type}",period,[team_type,'scores'])
                else:
                    raise ValueError(f'investigate period type "{period["periodType"]}""')        

def run_api_update(years, game_type = None):
    """apply the classes above to the requested years:"""
    games = []
    if game_type is None:
        gt = ['01','02','03']
    else:
        gt = [game_type]

    for year in years:
        for season_type in gt:
            if season_type in ['01','02']:
                foundLast = False
                gameid = int(f"{year}{season_type}0001")
                while not foundLast:          
                    game_object = get_nhl_game_data(str(gameid))

                    if game_object.hasData:
                        games.append(game_object)
                        gameid = gameid+1
                    else:
                        foundLast = True
            else:
                matchup_dict = {1:8,2:4,3:2,4:1}
                for roundnum,matchup_max in matchup_dict.items():
                    for matchup in range(matchup_max):
                        for game in range(7):
                            gameid = f"{year}{season_type}0{str(roundnum)}{str(matchup)}{str(game)}"
                            try:
                                game_object = get_nhl_game_data(gameid)
                                games.append(game_object)
                            except:
                                print(gameid)
                                raise ValueError()
    return games

def write_output(bucket_name, output_destination, write_mode, games):

    pd_officiating = pd.concat([game.df_officials for game in games if game.hasData and game.isComplete])
    pd_officiating.index = pd_officiating.game_id

    pd_game_info = pd.DataFrame([game.game_info for game in games if game.hasData and game.isComplete])
    pd_game_info.index = pd_game_info.game_pk

    pd_playerstats = pd.concat([game.df_playerstats for game in games if game.hasData and game.isComplete])
    pd_playerstats['index_val'] = pd_playerstats.index

    pd_playerinfo = pd.concat([game.df_players for game in games if game.hasData and game.isComplete])
    pd_playerinfo['index_val'] = pd_playerinfo.index

    # Some preseason games played in smaller venues do not have database IDs.
    pd_game_info['arena_id'].fillna(-999, inplace=True)

    df_game_info = spark.createDataFrame(pd_game_info.astype(str))
    df_playerstats = spark.createDataFrame(pd_playerstats.astype(str))
    df_playerinfo = spark.createDataFrame(pd_playerinfo.astype(str))
    df_officiating = spark.createDataFrame(pd_officiating.astype(str))

    a = ingest_nhl_teams()
    spark.createDataFrame(a.teams_df).write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/teams")
    df_game_info.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/gameinfo")
    df_playerstats.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/playerstats")
    df_playerinfo.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/playerinfo")
    df_officiating.write.format("parquet").mode(write_mode).option("overwriteSchema", "true").save(f"gs://{bucket_name}/{output_destination}/officials")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Ingestion to GCP")
    parser.add_argument("--yearmin", type=int, help="Starting NHL Season for data retrieval")
    parser.add_argument("--yearmax", type=int, help="Ending NHL Season for data retrieval")
    parser.add_argument("--game_type", type=str, help="PReaseason,Regular,or Playoff")
    parser.add_argument("--output_bucket", type=str, help="a GCS Bucket")
    parser.add_argument("--output_destination", type=str, help="a location within GCS bucket where output is stored")
    parser.add_argument("--write_mode", type=str, help="overwrite or append, as used in spark.write.*")
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("NHL Data Ingestion to GCP") \
        .getOrCreate()

    bucket_name = args.output_bucket
    output_destination = args.output_destination
    write_mode = args.write_mode

    _game_type = args.game_type
    if _game_type == 'PR':
        __game_type = '01'
    elif _game_type == 'R':
        __game_type = '02'
    else:
        __game_type = '03'

    yearmin = args.yearmin
    yearmax = args.yearmax
    years = [str(x) for x in range(int(yearmin), int(yearmax)+1)]

    games = run_api_update(years,__game_type)

    write_output(bucket_name, output_destination, write_mode, games)

    spark.stop()