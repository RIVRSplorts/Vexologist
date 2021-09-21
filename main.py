import json
import sqlite3
import re
from os import path, listdir
#requirements
#parse files that havn't been parsed before
# :add results to database?
# :record what files have been parsed

class vexologist(object):
    def __init__(self,database,racers_json_path='../json/racers'):

        self.racers_json_path = racers_json_path

        if not path.isfile(database):
            self.conn = sqlite3.connect(database)
            self.cur = self.conn.cursor()
            self.cur.execute('''CREATE TABLE "Racers" (
	    "Racer_ID"        INTEGER,
	    "Emoji"           TEXT UNIQUE,
	    "Name"	      TEXT,
            "Team"            TEXT,
            "Origins"         TEXT,
            "Races"           INTEGER DEFAULT 0,
            "Bonks"	      INTEGER DEFAULT 0,
	    "Failed_Bonks"    INTEGER DEFAULT 0,
	    "Ploughs"         INTEGER DEFAULT 0,
	    "Swerves"	      INTEGER DEFAULT 0,
	    "Tricks_Landed"   INTEGER DEFAULT 0,
	    "Tricks_Missed"   INTEGER DEFAULT 0,
	    "Tricks_Flipped"  INTEGER DEFAULT 0,
	    "Suns_Smile"      INTEGER DEFAULT 0,
	    "Clouds_Desc"     INTEGER DEFAULT 0,
            "urns_smashed"    INTEGER DEFAULT 0,
            "Firsts"          INTEGER DEFAULT 0,
            "Seconds"         INTEGER DEFAULT 0,
            "Thirds"          INTEGER DEFAULT 0,
            "Fourths"         INTEGER DEFAULT 0,
            "Fifths"          INTEGER DEFAULT 0,
            "Sixths"          INTEGER DEFAULT 0,
            "Sevenths"        INTEGER DEFAULT 0,
            "Eighths"         INTEGER DEFAULT 0,
            "Spice_mu"        REAL    DEFAULT 0,
            "Spice_sigma"     REAL    DEFAULT 0,
            "Stats_total"     REAL    DEFAULT 0,
            "Stat_ED"         REAL    DEFAULT 0,
            "Stat_BU"         REAL    DEFAULT 0,
            "Stat_VP"         REAL    DEFAULT 0,
            "Stat_LF"         REAL    DEFAULT 0,
            "Stat_CH"         REAL    DEFAULT 0,
            "Stat_CT"         REAL    DEFAULT 0,
            "Stat_HL"         REAL    DEFAULT 0,
            "Stat_SG"         REAL    DEFAULT 0,
            "Stat_MG"         REAL    DEFAULT 0,
            "Stat_EY"         REAL    DEFAULT 0,
            "Stat_AG"         REAL DEFAULT 0,
            PRIMARY KEY("Racer_ID")
            )''')

            self.cur.execute('''CREATE TABLE "Races" (
            "Race_ID"  INTEGER,
            "Cup_Name" TEXT,
            "Race_Num" INTEGER,
            PRIMARY KEY("Race_ID")
            )''')

            
        else:
            #has to be duplicated as otherwise sqlite3.connect creates the database before the if check
            self.conn = sqlite3.connect(database)
            self.cur = self.conn.cursor()

        #populate with racers
 
        self.update_racers()
          
        self.pit_stop= re.compile("(.*) waved from the Pit Stop!")
        self.bonk_S  = re.compile("bonked .* off the track")
        self.bonk_F  = re.compile("bonked into")
        self.plough  = re.compile("ploughed through a snowman")
        self.swerve  = re.compile("swerved to avoid a snowman")
        self.upgrade = re.compile("")
        self.trick_FO= re.compile("tried to do a .* but flipped out")
        self.trick_FL= re.compile("did a .* but missed the track")
        self.trick_S = re.compile("did a .*")
        self.smile   = re.compile("The sun smiled down on")
        self.cloud   = re.compile("The clouds descend on")
        self.steal   = re.compile("(.*) stole (.*) license")
        self.voided  = re.compile("(.*) disappeared into the void. (.*) emerges")

        self.urn_smashed = re.compile("smashed an urn")
        self.beelspin    = re.compile("The bees spin the Bee-l of Fortune!")

        self.roguemarshaled = re.compile("Rogue Marshal revoked (.*?) license!")
        self.replacement    = re.compile("\*\*(.*? .*?) (.*?)")#too loose to be practical

        #extract name in line-up format
        self.name_lineup = re.compile("\*\*(.*?)\*\*")

        #extract emoji and name in the event format
        self.race_name = re.compile("\*\*(.*?)\*\*")

        self.teams = []

        self.temp_total_ref = ["bonks","bonks_failed","ploughs","swerves","tricks landed",
                               "tricks missed","tricks flipped","sun shine", "cloud descend",
                               "urns smashed","1st","2nd","3rd","4th",
                               "5th","6th","7th","8th"]

    def insert_racer(self,emoji,name='',team='',pitstop=False):
        self.cur.execute("SELECT * FROM racers WHERE Emoji = ?",(emoji,))
        ret = self.cur.fetchone()
        #if there's an empty return, setup their record. note the defaults for stats are 0 so this also populates those columns implicitly
        #if return isn't empty, convert the return tuple to a list and stow it in the dict
        if not ret:
            self.update_racers()
            racer_temp_totals = [0,0,0,0,0, 0,0,0,0,0]
        else:
            racer_temp_totals = list(ret[6:])
            #ignore which team they're on during parsing as that gets updated at the end
            if pitstop:
                raced = 0
            else:
                raced = 1
            self.cur.execute("UPDATE Racers SET Races =? WHERE Emoji = ?",(ret[5]+raced,emoji))

        self.conn.commit()
        return racer_temp_totals

    
    def update_racers(self):
        #update or insert all record of active racers
        last_update = sorted(listdir(self.racers_json_path))[-1]
        with open(path.join(self.racers_json_path,last_update), 'r') as f:
            racers_file = json.load(f)
            
        allplayer_list = [racers_file["inactive"],racers_file["active"]]

        for active_status, player_sublist in enumerate(allplayer_list):
            for player in player_sublist.items():
                name = player[0]
                emoji = player[1]["emoji"]
                origin = player[1]["origins"]
                if "team" in player[1]:
                    team  = player[1]["team"]
                else:
                    team = None
                self.cur.execute("INSERT OR IGNORE into Racers (Emoji,Team, Name, Origins) VALUES (?,?,?,?)",(emoji,team,name,origin))
        self.conn.commit()

    def update_racer_stats(self):
        #update the stats which aren't used by Vex for parsing but are useful when looking at the data
        #also updates the players team for convenience
        last_update = sorted(listdir(self.racers_json_path))[-1]
        with open(path.join(self.racers_json_path,last_update), 'r') as f:
            racers_file = json.load(f)
            
        allplayer_list = [racers_file["inactive"],racers_file["active"]]
        for active_status, player_sublist in enumerate(allplayer_list):
            for player in player_sublist.items():
                name = player[0]
                stats = player[1]['stats']
                total_stats = sum(stats.values())
                if "team" in player[1]:
                    team = player[1]["team"]
                else:
                    team = None
                    
                if 'spice' in player[1]:
                    spice = player[1]['spice']
                else:
                    #incase an old season is read in that predates spice
                    spice = {"mu":None,"sigma":None}
                self.cur.execute("UPDATE Racers SET Team =?, Spice_mu =?,Spice_sigma =?, Stats_total =?,Stat_ED =?,Stat_BU =?,Stat_VP =?,Stat_LF =?,"+
                         "Stat_CH =?,Stat_CT =?,Stat_HL =?,Stat_SG =?,Stat_MG =?,"+
                         "Stat_EY =?,Stat_AG =? WHERE Name = ?",(team,spice["mu"],spice["sigma"],total_stats,stats['ED'],stats['BU'],stats['VP'],stats['LF'],stats['CH'],stats['CT'],stats['HL'],stats['SG'],stats['MG'],stats['EY'],stats['AG'],name))
        self.conn.commit()
        
    #parse race and update racers records
    def parse_race(self,race_json):
        colnames = ["Firsts","Seconds","Thirds","Fourths","Fifths","Sixths","Sevenths","Eighths"]
        cup    = race_json["cup"]["name"]
        race_n = race_json["cup"]["racenum"]


        feed = race_json["feed"]

        racers_temp_totals = {}
        events_temp_total = {}

        
        #check if this race has been parsed before
        self.cur.execute("SELECT * FROM Races WHERE Cup_Name = ? AND Race_Num = ?", (cup, race_n))
        ret = self.cur.fetchone()
        if ret != None:
            #print("race already read in")
            return 1
        else:
            self.cur.execute("INSERT INTO Races (Cup_Name, Race_Num) VALUES (?,?)",(cup, race_n))
            print(cup, race_n+1)
        
        #parse racers in this race
        racers = feed[1:9]
        for line in feed[1:9]:
            #flag if a player swap happens

            #use emoji as a unique key to check if they've previously been seen / have stats 
            team  = line[0]
            name = self.name_lineup.search(line).group(1)
            emoji = line.split(' ')[1]
            racers_temp_totals[emoji] = self.insert_racer(emoji,name,team)
        #commit racer insertions

        
        player_swapped_next_line = False 
        for line in feed[9:]:
            #print(line)

            name_emoji = self.race_name.search(line)
            bee_check  = self.beelspin.search(line)

            if name_emoji == None or bee_check:
                #if line doesn't contain a name match, return
                #or if line has a beel of fortune spin, which false match with the naming highlights
                #check if the line isa  steps in line after a rogue marshal 

                continue

            name_emoji = name_emoji.group(1)
            emoji = name_emoji.split(' ')[0]
            name  = name_emoji.split(' ')[1]
            
            if self.pit_stop.search(line):
                #pit_re =  self.pit_stop.search(line)
                #name = self.race_name.search(line).group(0)[4:-2]
                self.insert_racer(emoji,pitstop=True)

            elif self.roguemarshaled.search(line):
                pass #currently not anything to do if this happens


            elif self.steal.search(line):
                #add new player with all zeros
                #steal_re = self.steal.search(line)
                #stolen_from = steal_re.group(2)[0]
                steal_split = line.split(' ')
                emoji = steal_split[0][2:]
                stolen_from = steal_split[3]
                               
                racers_temp_totals[emoji] = self.insert_racer(emoji)#need to link this to peeps.json to pull out who it is

                
            elif self.voided.search(line):
                #add new player with all zeros
                #void_re = self.voided.search(line)
                #voided = void_re.group(1)[2]
                #emoji = void_re.group(2)[0]
                void_split = line.split(' ')
                voided = void_split[0][2:]
                emoji = void_split[6]
                racers_temp_totals[emoji] =  self.insert_racer(emoji)#need to link this to peeps.json to pull out who it is

                
            elif self.bonk_S.search(line):
                #print("succesful bonk")
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][0] += 1
                
            elif self.bonk_F.search(line):
                #print("bonk failed")
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][1] += 1
                
            elif self.plough.search(line):
                #print("ploughed")
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][2] +=1
                
            elif self.swerve.search(line):
                #print("sweved")
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][3] +=1
                
            elif self.trick_FO.search(line):
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                #print("failed trick landing")
                racers_temp_totals[emoji][4] +=1
                
            elif self.trick_FL.search(line):
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                #print("failed trick missed")
                racers_temp_totals[emoji][5] +=1
                
            elif self.trick_S.search(line):
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                #print("trick success")
                racers_temp_totals[emoji][6] +=1
                
            elif self.smile.search(line):
                #print("sun smiled")
                emoji = line.split(' ')[5] #sun smiles has emoji in weird place
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][7] +=1
                
            elif self.cloud.search(line):
                #print("clouded")
                emoji = line.split(' ')[4]#clouds descends has emoji in a weird place
                if emoji not in racers_temp_totals:
                    racers_temp_totals[emoji] = self.insert_racer(emoji)
                racers_temp_totals[emoji][8] +=1
                
            elif self.urn_smashed.search(line):
                racers_temp_totals[emoji][9] +=1

        for racer_temp in racers_temp_totals.items():
            #print(racer_temp[0],racer_temp[1])
            self.cur.execute("UPDATE Racers SET Bonks=?, Failed_Bonks=?,Ploughs=?,Swerves=?, Tricks_Flipped=?, Tricks_Missed=?, Tricks_Landed=?,Suns_Smile=?,Clouds_Desc=?,urns_smashed=? WHERE Emoji = ?",(racer_temp[1][:10]+[racer_temp[0]]))
            #if last race of the cup
        if race_n == 3:
            try:
                results = race_json["cupranking"]
                for i, colname in enumerate(colnames):
                    self.cur.execute("SELECT %s From Racers WHERE Name = ?"%(colname,),(results[i],))
                    ret = self.cur.fetchone()[0]
                    self.cur.execute("UPDATE Racers SET (%s)= ? WHERE Name =?"%(colname,),(ret+1,results[i]))
            except KeyError:
                print('data missing cup ranking!')
            except IndexError:
                print('uhh we lost a player somewhere chief')

        self.conn.commit()

            
if __name__ == "__main__":
    #test = "../Vexologist/The Cider Gravy Boat_2_2021-08-20 22:12:16.json"

    database = './Season.db'

    datahandler = vexologist(database, '../json/racers/')

    #test = "../Vexologist/The Whoop-ass Jug_4_2021-08-21 11:14:17.json"
    #with open(test, 'r') as f:
    #    test_data = json.load(f) 
    #datahandler.parse_race(test_data)

    data_dir = "../json/races/"
    
    datahandler.update_racers()
    datahandler.update_racer_stats()
    
    for file_n in listdir(data_dir):
        file_full = path.join(data_dir,file_n)
        try:
            with open(file_full, 'r') as f:
                json_dump = json.load(f)
            datahandler.parse_race(json_dump)
        except IsADirectoryError:
            pass
    

    
