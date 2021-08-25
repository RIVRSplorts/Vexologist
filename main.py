import json
import sqlite3
import re
from os import path, listdir
#requirements
#parse files that havn't been parsed before
# :add results to database?
# :record what files have been parsed





class vexologist(object):
    def __init__(self,database):


        if not path.isfile(database):
            self.conn = sqlite3.connect(database)
            self.cur = self.conn.cursor()
            self.cur.execute('''CREATE TABLE "Racers" (
	    "Racer_ID"        INTEGER,
	    "Emoji"           TEXT UNIQUE,
	    "Name"	      TEXT,
            "Team"            TEXT,
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

        self.name = re.compile("\*\*.*\*\*")

        self.teams = []

        self.temp_total_ref = ["bonks","bonks_failed","ploughs","swerves","tricks landed",
                               "tricks missed","tricks flipped","sun shine", "cloud descend",
                               "urns smashed","1st","2nd","3rd","4th",
                               "5th","6th","7th","8th"]

    def insert_racer(self,emoji,name,team):
        self.cur.execute("SELECT * FROM racers WHERE Emoji = ?",(emoji,))
        ret = self.cur.fetchone()
        #if there's an empty return, setup their record. note the defaults for stats are 0 so this also populates those columns implicitly
        #if return isn't empty, convert the return tuple to a list and stow it in the dict
        if not ret:
            self.update_racers()
            racer_temp_totals = [0,0,0,0,0, 0,0,0,0,0]
        else:
            racer_temp_totals = list(ret[5:])
            #ignore which team they're on during parsing as that gets updated at the end
            self.cur.execute("UPDATE Racers SET Races =? WHERE Emoji = ?",(ret[4]+1,emoji))

        self.conn.commit()
        return racer_temp_totals

    
    def update_racers(self):
        #update or insert all record of active racers
        last_update = sorted(listdir('../json/racers/'))[-1]
        with open(path.join('../json/racers/',last_update), 'r') as f:
            racers_file = json.load(f)
            
        allplayer_list = [racers_file["inactive"],racers_file["active"]]

        for active_status, player_sublist in enumerate(allplayer_list):
            for player in player_sublist.items():
                name = player[0]
                emoji = player[1]["emoji"]
                if "team" in player[1]:
                    team  = player[1]["team"]
                else:
                    team = None
                self.cur.execute("INSERT OR IGNORE into Racers (Emoji,Team, Name) VALUES (?,?,?)",(emoji,team,name))
        self.conn.commit()


    #parse race and update racers records
    def parse_race(self,race_json):
        colnames = ["Firsts","Seconds","Thirds","Fourths","Fifths","Sixths","Sevenths","Eighths"]
        cup    = race_json["cup"]["name"]
        race_n = race_json["cup"]["racenum"]


        feed = race_json["feed"]

        racers_temp_totals = {}
        events_temp_total = {}

        #parse racers in this race
        racers = feed[1:9]
        for line in feed[1:9]:
            #use emoji as a unique key to check if they've previously been seen / have stats 
            emoji = line[2]
            team  = line[0]
            name = self.name.search(line).group(0)[2:-2]
            racers_temp_totals[emoji] = self.insert_racer(emoji,name,team)
        #commit racer insertions

        
        
        #check if this race has been parsed before
        self.cur.execute("SELECT * FROM Races WHERE Cup_Name = ? AND Race_Num = ?", (cup, race_n))
        ret = self.cur.fetchone()
        if ret != None:
            print("race already read in")
            return 1
        else:
            self.cur.execute("INSERT INTO Races (Cup_Name, Race_Num) VALUES (?,?)",(cup, race_n))
            print(cup, race_n+1)
        
        
        for line in feed[9:]:
            #print(line)
            emoji = line[2]
            if self.pit_stop.search(line):
                #pit_re =  self.pit_stop.search(line)
                name = self.name.search(line).group(0)[4:-2]
                self.insert_racer(line[2],name,' ')
                
            elif self.steal.search(line):
                #add new player with all zeros
                steal_re = self.steal.search(line)
                stolen_from = steal_re.group(2)[0]
                racers_temp_totals[line[2]] = [0,0,0,0,0, 0,0,0,0,0]
                self.insert_racer(line[2],'new_racer?',None)#need to link this to peeps.json to pull out who it is

                
            elif self.voided.search(line):
                #add new player with all zeros
                void_re = self.voided.search(line)
                voided = void_re.group(1)[2]
                emoji = void_re.group(2)[0]
                racers_temp_totals[emoji] = [0,0,0,0,0, 0,0,0,0,0]
                self.insert_racer(emoji,'new_racer?',None)#need to link this to peeps.json to pull out who it is

                
            elif self.bonk_S.search(line):
                #print("succesful bonk")
                racers_temp_totals[emoji][0] += 1
            elif self.bonk_F.search(line):
                #print("bonk failed")
                racers_temp_totals[emoji][1] += 1
            elif self.plough.search(line):
                #print("ploughed")
                racers_temp_totals[emoji][2] +=1
            elif self.swerve.search(line):
                #print("sweved")
                racers_temp_totals[emoji][3] +=1
            elif self.trick_FO.search(line):
                #print("failed trick landing")
                racers_temp_totals[emoji][4] +=1
            elif self.trick_FL.search(line):
                #print("failed trick missed")
                racers_temp_totals[emoji][5] +=1
            elif self.trick_S.search(line):
                #print("trick success")
                racers_temp_totals[emoji][6] +=1
            elif self.smile.search(line):
                #print("sun smiled")
                emoji = line[25] #sun smiles has emoji in weird place
                racers_temp_totals[emoji][7] +=1
            elif self.cloud.search(line):
                #print("clouded")
                emoji = line[24] #clouds descends has emoji in a weird place
                racers_temp_totals[emoji][8] +=1
            elif self.urn_smashed.search(line):
                #not clear what this does yet
                racers_temp_totals[emoji][9] +=1

        for racer_temp in racers_temp_totals.items():
            #print(racer_temp[0],racer_temp[1])
            self.cur.execute("UPDATE Racers SET Bonks=?, Failed_Bonks=?,Ploughs=?,Swerves=?, Tricks_Landed=?, Tricks_Missed=?, Tricks_Flipped=?,Suns_Smile=?,Clouds_Desc=? WHERE Emoji = ?",(racer_temp[1][:9]+[racer_temp[0]]))
            #if last race of the cup
        if race_n == 3:
            results = race_json["cupranking"]
            for i, colname in enumerate(colnames):
                self.cur.execute("SELECT %s From Racers WHERE Name = ?"%(colname,),(results[i],))
                ret = self.cur.fetchone()[0]
                self.cur.execute("UPDATE Racers SET (%s)= ? WHERE Name =?"%(colname,),(ret+1,results[i]))
            
        self.conn.commit()

            
if __name__ == "__main__":
    #test = "../Vexologist/The Cider Gravy Boat_2_2021-08-20 22:12:16.json"

    database = './vexbase.db'

    datahandler = vexologist(database)

    
    #test = "../Vexologist/The Whoop-ass Jug_4_2021-08-21 11:14:17.json"
    #with open(test, 'r') as f:
    #    test_data = json.load(f) 
    #datahandler.parse_race(test_data)

    data_dir = "../json/races"

    for file_n in listdir(data_dir):
        file_full = path.join(data_dir,file_n)
        with open(file_full, 'r') as f:
            json_dump = json.load(f)
        datahandler.parse_race(json_dump)
    

    datahandler.update_racers()
