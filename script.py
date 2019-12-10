def FilterSimilarNames(similarity, name, cutoff, data):
    f1 = similarity[(similarity.str1 == name) & (similarity.measure > cutoff)]['str2']
    f2 = similarity[(similarity.str2 == name) & (similarity.measure > cutoff)]['str1']
    similarnames = f1.append(f2).reset_index(drop=True)
    similarnames = pandas.DataFrame(similarnames, columns=['fullname_alias_adj'])
    # add middle name code.
    # create an extra dummy where this creates a change..
    mark = False
    if name.find(',') > -1 and name.split(',')[1].strip().find(' ') > -1 and name.split(',')[1].strip().find(')') == -1:
        for n in name.split(',')[1].strip().split(' '):
            if n != '':
                if len(data[(data['firstname_alias_adj'] == n) & (
                        data['lastname_alias_adj'] == name.split(',')[0])]) > 1:
                    similarnames = similarnames.append(data[(data['firstname_alias_adj'] == name) & (
                                data['lastname_alias_adj'] == name.split(',')[0])]['fullname_alias_adj'])
                    mark = True
    return data.merge(similarnames, on='fullname_alias_adj').drop_duplicates().reset_index(drop=True), mark


def FilterSimilarNamesConsiderCompanies(similarity, name, cutoff, df_tobefiltered, is_company):
    if is_company == 1:
        f1 = similarity[(similarity.str1 == name) & (similarity.measure > 0.9)]['str2']
        f2 = similarity[(similarity.str2 == name) & (similarity.measure > 0.9)]['str1']
    else:
        f1 = similarity[(similarity.str1 == name) & (similarity.measure > cutoff)]['str2']
        f2 = similarity[(similarity.str2 == name) & (similarity.measure > cutoff)]['str1']
    similarnames = f1.append(f2).reset_index(drop=True)
    similarnames = pandas.DataFrame(similarnames, columns=['fullname_alias_adj'])
    return df_tobefiltered.merge(similarnames, on='fullname_alias_adj').drop_duplicates().reset_index(drop=True)


def GetCollaboratorsOfRow(k, data):
    lcol = []
    for column in data.columns.tolist():
        if column.startswith('collaborator') and column.endswith('_adj'):
            if pandas.notnull(data.loc[k, column])  and str(data.loc[k, column]).strip() != '':
                lcol.append(data.loc[k, column])
    return list(set(lcol))


def FilterPortCollaboratorShipname(dataframe, collaborators, portdep, shipname, alg):
    dataframe['check' + '_' + alg] = False
    for k in range(0, len(dataframe)):
        lcol = GetCollaboratorsOfRow(k, dataframe)
        if (dataframe.loc[k, 'portdep'] == portdep) \
                or (len(set(collaborators).intersection(set(lcol))) > 0) \
                or (dataframe.loc[k, 'shipname'] == shipname):
            dataframe.loc[k, 'check' + '_' + alg] = True
    dataframe = dataframe[dataframe['check' + '_' + alg]]
    dataframe.drop(['check' + '_' + alg], axis=1)
    return dataframe


def FilterDateDep(dataframe, datedep_range_first, datedep_range_last, datedep_range_mean):
    return dataframe[((datedep_range_first - dataframe['datedep_range_first']) > -6) \
                     & ((dataframe['datedep_range_last'] - datedep_range_last) < 6) \
                     & ((dataframe['datedep_range_mean'] - datedep_range_mean) < 16) \
                     & ((dataframe['datedep_range_mean'] - datedep_range_mean) > -16)]


def FISOMF(data, last_name, initial):
    if pandas.isnull(initial):
        return data[(data['lastname_alias_adj'] == last_name)]
    else:
        return data[(data['lastname_alias_adj'] == last_name) \
                    & \
                    ( \
                                ( \
                                            (data['firstname_alias_adj'].str.get(0) == initial) \
                                            & \
                                            (data['dummy_firstname_recognized'] == 0) \
                                    ) \
                                | \
                                (data['dummy_missing_firstname'] == 1) \
                        )]


def CreateMid(data, i, filtered, cutoff, alg):
    g = i
    try:
        g = filtered[pandas.notnull(filtered['mid_' + str(cutoff) + '_' + alg])][
            'mid_' + str(cutoff) + '_' + alg].reset_index(drop=True)[0]
    except:
        pass
    for ci in filtered['ci_' + alg].unique().tolist():
        data.loc[ci, 'mid_' + str(cutoff) + '_' + alg] = g
    data.loc[i, 'mid_' + str(cutoff) + '_' + alg] = g


def FilterFatherSonDummies(is_father, is_son, dataframe):
    '''
    if is_father == 1:
        dataframe = dataframe[dataframe['dummy_son'] == 0]
    if is_son == 1:
        dataframe = dataframe[dataframe['dummy_father'] == 0]
    '''
    # algorithm B does nothing..
    return dataframe.reset_index(drop=True)

def FilterKindredDummies(is_father, is_son, is_eldest, is_brother, is_brothers, dataframe):
    if is_father:
        dataframe = dataframe[dataframe['dummy_father']==1]
    elif is_son:
        dataframe = dataframe[dataframe['dummy_son']==1]
    elif is_brother:
        dataframe = dataframe[dataframe['dummy_brother']==1]
    elif is_brothers:
        dataframe = dataframe[dataframe['dummy_brothers']==1]
    elif is_eldest:
        dataframe = dataframe[dataframe['dummy_eldest']==1]
    else:
        dataframe = dataframe[(dataframe['dummy_brothers']==0) & (dataframe['dummy_brother']==0) & (dataframe['dummy_son']==0) & (dataframe['dummy_eldest']==0) & (dataframe['dummy_father']==0)]
    return dataframe.reset_index(drop=True)


def GetVariablesFor(data, i):
    row1name = data.loc[i, 'fullname_alias_adj']
    portdep = data.loc[i, 'portdep']
    shipname = data.loc[i, 'shipname']
    datedep_range_first = data.loc[i, 'datedep_range_first']
    datedep_range_last = data.loc[i, 'datedep_range_last']
    datedep_range_mean = data.loc[i, 'datedep_range_mean']
    last_name = data.loc[i, 'lastname_alias_adj']
    initial = data['firstname_alias_adj'].str.get(0)[i]
    is_company = data.loc[i, 'dummy_company']
    is_father = data.loc[i, 'dummy_father']
    is_son = data.loc[i, 'dummy_son']
    is_eldest = data.loc[i, 'dummy_eldest']
    is_brother = data.loc[i, 'dummy_brother']
    is_brothers = data.loc[i, 'dummy_brothers']
    collaborators = GetCollaboratorsOfRow(i, data)
    return row1name, portdep, shipname, datedep_range_first, datedep_range_last, \
           datedep_range_mean, last_name, initial, is_company, is_father, is_eldest,\
           is_brother, is_brothers, is_son, collaborators


################################################################################
# Script Starts Here
################################################################################

# library for dataframe manipulation and storage
# python library corresponding to stata
import pandas

# function for measuring similarity between two strings:
# https://docs.python.org/2/library/difflib.html
from difflib import SequenceMatcher

################################################################################
# import file as pandas Dataframe from path
original_data_path = 'Data\\Orginal SV database_14043_merged_precleaned.dta'
data = pandas.read_stata(original_data_path)
# running the script for rows between 400-600 rows.. (if you want to do quick check for smaller part)
# data = data[1400:1500]

# 1a- removing unused columns from data.  (refer 1b below)
data = data[['fullname_alias_adj', 'collaborator1_adj', 'collaborator2_adj', 'collaborator3_adj', \
             'collaborator4_adj', 'collaborator5_adj', 'collaborator6_adj', 'collaborator7_adj', 'collaborator8_adj', \
             'collaborator9_adj', 'collaborator10_adj', 'collaborator11_adj', 'collaborator12_adj', \
             'collaborator13_adj', 'collaborator14_adj', 'collaborator15_adj', 'datedep_range_first', \
             'datedep_range_last', 'datedep_range_mean', 'lastname_alias_adj', 'firstname_alias_adj', \
             'dummy_company', 'dummy_father', 'dummy_son', 'dummy_missing_firstname', 'portdep', 'shipname', \
             'dummy_eldest','dummy_brother','dummy_brothers']] # extra columns are now used for algorithm G

# allègre and allegre s similarity was 0.8 in similarity matrix, so we are changing letters with accents..
data['fullname_alias_adj'] = data['fullname_alias_adj'].str.replace('è', 'e').str.replace('é', 'e').str.replace('î','i').str.replace('ç', 'c').str.replace('ë', 'e').str.replace('É', 'E')
# do the same thing for collaborators.
for column in data.columns.tolist():
    if column.startswith('collaborator') and column.endswith('_adj'):
        data[column] = data[column].str.replace('è', 'e').str.replace('é', 'e').str.replace('î', 'i').str.replace('ç','c').str.replace('ë', 'e').str.replace('É', 'E')

# creating a dummy for recognized names..
recognized_names = ['james', 'john', 'robert', 'michael', 'william', 'david', 'richard', 'charles', 'joseph', 'thomas',
                    'christopher', 'daniel', 'paul', 'mark', 'donald', 'george', 'kenneth', 'steven', 'edward', 'brian',
                    'ronald', 'anthony', 'kevin', 'jason', 'matthew', 'gary', 'timothy', 'jose', 'larry', 'jeffrey',
                    'frank', 'scott', 'eric', 'stephen', 'andrew', 'raymond', 'gregory', 'joshua', 'jerry', 'dennis',
                    'walter', 'patrick', 'peter', 'harold', 'douglas', 'henry', 'carl', 'arthur', 'ryan', 'roger',
                    'joe', 'juan', 'jack', 'albert', 'jonathan', 'justin', 'terry', 'gerald', 'keith', 'samuel',
                    'willie', 'ralph', 'lawrence', 'nicholas', 'roy', 'benjamin', 'bruce', 'brandon', 'adam', 'harry',
                    'fred', 'wayne', 'billy', 'steve', 'louis', 'jeremy', 'aaron', 'randy', 'howard', 'eugene',
                    'carlos', 'russell', 'bobby', 'victor', 'martin', 'ernest', 'phillip', 'todd', 'jesse', 'craig',
                    'alan', 'shawn', 'clarence', 'sean', 'philip', 'chris', 'johnny', 'earl', 'jimmy', 'antonio',
                    'danny', 'bryan', 'tony', 'luis', 'mike', 'stanley', 'leonard', 'nathan', 'dale', 'manuel',
                    'rodney', 'curtis', 'norman', 'allen', 'marvin', 'vincent', 'glenn', 'jeffery', 'travis', 'jeff',
                    'chad', 'jacob', 'lee', 'melvin', 'alfred', 'kyle', 'francis', 'bradley', 'jesus', 'herbert',
                    'frederick', 'ray', 'joel', 'edwin', 'don', 'eddie', 'ricky', 'troy', 'randall', 'barry',
                    'alexander', 'bernard', 'mario', 'leroy', 'francisco', 'marcus', 'micheal', 'theodore', 'clifford',
                    'miguel', 'oscar', 'jay', 'jim', 'tom', 'calvin', 'alex', 'jon', 'ronnie', 'bill', 'lloyd', 'tommy',
                    'leon', 'derek', 'warren', 'darrell', 'jerome', 'floyd', 'leo', 'alvin', 'tim', 'wesley', 'gordon',
                    'dean', 'greg', 'jorge', 'dustin', 'pedro', 'derrick', 'dan', 'lewis', 'zachary', 'corey', 'herman',
                    'maurice', 'vernon', 'roberto', 'clyde', 'glen', 'hector', 'shane', 'ricardo', 'sam', 'rick',
                    'lester', 'brent', 'ramon', 'charlie', 'tyler', 'gilbert', 'gene', 'marc', 'reginald', 'ruben',
                    'brett', 'angel', 'nathaniel', 'rafael', 'leslie', 'edgar', 'milton', 'raul', 'ben', 'chester',
                    'cecil', 'duane', 'franklin', 'andre', 'elmer', 'brad', 'gabriel', 'ron', 'mitchell', 'roland',
                    'arnold', 'harvey', 'jared', 'adrian', 'karl', 'cory', 'claude', 'erik', 'darryl', 'jamie', 'neil',
                    'jessie', 'christian', 'javier', 'fernando', 'clinton', 'ted', 'mathew', 'tyrone', 'darren',
                    'lonnie', 'lance', 'cody', 'julio', 'kelly', 'kurt', 'allan', 'nelson', 'guy', 'clayton', 'hugh',
                    'max', 'dwayne', 'dwight', 'armando', 'felix', 'jimmie', 'everett', 'jordan', 'ian', 'wallace',
                    'ken', 'bob', 'jaime', 'casey', 'alfredo', 'alberto', 'dave', 'ivan', 'johnnie', 'sidney', 'byron',
                    'julian', 'isaac', 'morris', 'clifton', 'willard', 'daryl', 'ross', 'virgil', 'andy', 'marshall',
                    'salvador', 'perry', 'kirk', 'sergio', 'marion', 'tracy', 'seth', 'kent', 'terrance', 'rene',
                    'eduardo', 'terrence', 'enrique', 'freddie', 'wade', 'austin', 'stuart', 'fredrick', 'arturo',
                    'alejandro', 'jackie', 'joey', 'nick', 'luther', 'wendell', 'jeremiah', 'evan', 'julius', 'dana',
                    'donnie', 'otis', 'shannon', 'trevor', 'oliver', 'luke', 'homer', 'gerard', 'doug', 'kenny',
                    'hubert', 'angelo', 'shaun', 'lyle', 'matt', 'lynn', 'alfonso', 'orlando', 'rex', 'carlton',
                    'ernesto', 'cameron', 'neal', 'pablo', 'lorenzo', 'omar', 'wilbur', 'blake', 'grant', 'horace',
                    'roderick', 'kerry', 'abraham', 'willis', 'rickey', 'jean', 'ira', 'andres', 'cesar', 'johnathan',
                    'malcolm', 'rudolph', 'damon', 'kelvin', 'rudy', 'preston', 'alton', 'archie', 'marco', 'wm',
                    'pete', 'randolph', 'garry', 'geoffrey', 'jonathon', 'felipe', 'bennie', 'gerardo', 'ed', 'dominic',
                    'robin', 'loren', 'delbert', 'colin', 'guillermo', 'earnest', 'lucas', 'benny', 'noel', 'spencer',
                    'rodolfo', 'myron', 'edmund', 'garrett', 'salvatore', 'cedric', 'lowell', 'gregg', 'sherman',
                    'wilson', 'devin', 'sylvester', 'kim', 'roosevelt', 'israel', 'jermaine', 'forrest', 'wilbert',
                    'leland', 'simon', 'guadalupe', 'clark', 'irving', 'carroll', 'bryant', 'owen', 'rufus', 'woodrow',
                    'sammy', 'kristopher', 'mack', 'levi', 'marcos', 'gustavo', 'jake', 'lionel', 'marty', 'taylor',
                    'ellis', 'dallas', 'gilberto', 'clint', 'nicolas', 'laurence', 'ismael', 'orville', 'drew', 'jody',
                    'ervin', 'dewey', 'al', 'wilfred', 'josh', 'hugo', 'ignacio', 'caleb', 'tomas', 'sheldon', 'erick',
                    'frankie', 'stewart', 'doyle', 'darrel', 'rogelio', 'terence', 'santiago', 'alonzo', 'elias',
                    'bert', 'elbert', 'ramiro', 'conrad', 'pat', 'noah', 'grady', 'phil', 'cornelius', 'lamar',
                    'rolando', 'clay', 'percy', 'dexter', 'bradford', 'merle', 'darin', 'amos', 'terrell', 'moses',
                    'irvin', 'saul', 'roman', 'darnell', 'randal', 'tommie', 'timmy', 'darrin', 'winston', 'brendan',
                    'toby', 'van', 'abel', 'dominick', 'boyd', 'courtney', 'jan', 'emilio', 'elijah', 'cary', 'domingo',
                    'santos', 'aubrey', 'emmett', 'marlon', 'emanuel', 'jerald', 'edmond', 'emil', 'dewayne', 'will',
                    'otto', 'teddy', 'reynaldo', 'bret', 'morgan', 'jess', 'trent', 'humberto', 'emmanuel', 'stephan',
                    'louie', 'vicente', 'lamont', 'stacy', 'garland', 'miles', 'micah', 'efrain', 'billie', 'logan',
                    'heath', 'rodger', 'harley', 'demetrius', 'ethan', 'eldon', 'rocky', 'pierre', 'junior', 'freddy',
                    'eli', 'bryce', 'antoine', 'robbie', 'kendall', 'royce', 'sterling', 'mickey', 'chase', 'grover',
                    'elton', 'cleveland', 'dylan', 'chuck', 'damian', 'reuben', 'stan', 'august', 'leonardo', 'jasper',
                    'russel', 'erwin', 'benito', 'hans', 'monte', 'blaine', 'ernie', 'curt', 'quentin', 'agustin',
                    'murray', 'jamal', 'devon', 'adolfo', 'harrison', 'tyson', 'burton', 'brady', 'elliott', 'wilfredo',
                    'bart', 'jarrod', 'vance', 'denis', 'damien', 'joaquin', 'harlan', 'desmond', 'elliot', 'darwin',
                    'ashley', 'gregorio', 'buddy', 'xavier', 'kermit', 'roscoe', 'esteban', 'anton', 'solomon',
                    'scotty', 'norbert', 'elvin', 'williams', 'nolan', 'carey', 'rod', 'quinton', 'hal', 'brain', 'rob',
                    'elwood', 'kendrick', 'darius', 'moises', 'son', 'marlin', 'fidel', 'thaddeus', 'cliff', 'marcel',
                    'ali', 'jackson', 'raphael', 'bryon', 'armand', 'alvaro', 'jeffry', 'dane', 'joesph', 'thurman',
                    'ned', 'sammie', 'rusty', 'michel', 'monty', 'rory', 'fabian', 'reggie', 'mason', 'graham', 'kris',
                    'isaiah', 'vaughn', 'gus', 'avery', 'loyd', 'diego', 'alexis', 'adolph', 'norris', 'millard',
                    'rocco', 'gonzalo', 'derick', 'rodrigo', 'gerry', 'stacey', 'carmen', 'wiley', 'rigoberto',
                    'alphonso', 'ty', 'shelby', 'rickie', 'noe', 'vern', 'bobbie', 'reed', 'jefferson', 'elvis',
                    'bernardo', 'mauricio', 'hiram', 'donovan', 'basil', 'riley', 'ollie', 'nickolas', 'maynard',
                    'scot', 'vince', 'quincy', 'eddy', 'sebastian', 'federico', 'ulysses', 'heriberto', 'donnell',
                    'cole', 'denny', 'davis', 'gavin', 'emery', 'ward', 'romeo', 'jayson', 'dion', 'dante', 'clement',
                    'coy', 'odell', 'maxwell', 'jarvis', 'bruno', 'issac', 'mary', 'dudley', 'brock', 'sanford',
                    'colby', 'carmelo', 'barney', 'nestor', 'hollis', 'stefan', 'donny', 'art', 'linwood', 'beau',
                    'weldon', 'galen', 'isidro', 'truman', 'delmar', 'johnathon', 'silas', 'frederic', 'dick', 'kirby',
                    'irwin', 'cruz', 'merlin', 'merrill', 'charley', 'marcelino', 'lane', 'harris', 'cleo', 'carlo',
                    'trenton', 'kurtis', 'hunter', 'aurelio', 'winfred', 'vito', 'collin', 'denver', 'carter', 'leonel',
                    'emory', 'pasquale', 'mohammad', 'mariano', 'danial', 'blair', 'landon', 'dirk', 'branden', 'adan',
                    'numbers', 'clair', 'buford', 'german', 'bernie', 'wilmer', 'joan', 'emerson', 'zachery',
                    'fletcher', 'jacques', 'errol', 'dalton', 'monroe', 'josue', 'dominique', 'edwardo', 'booker',
                    'wilford', 'sonny', 'shelton', 'carson', 'theron', 'raymundo', 'daren', 'tristan', 'houston',
                    'robby', 'lincoln', 'jame', 'genaro', 'gale', 'bennett', 'octavio', 'cornell', 'laverne', 'hung',
                    'arron', 'antony', 'herschel', 'alva', 'giovanni', 'garth', 'cyrus', 'cyril', 'ronny', 'stevie',
                    'lon', 'freeman', 'erin', 'duncan', 'kennith', 'carmine', 'augustine', 'young', 'erich', 'chadwick',
                    'wilburn', 'russ', 'reid', 'myles', 'anderson', 'morton', 'jonas', 'forest', 'mitchel', 'mervin',
                    'zane', 'rich', 'jamel', 'lazaro', 'alphonse', 'randell', 'major', 'johnie', 'jarrett', 'brooks',
                    'ariel', 'abdul', 'dusty', 'luciano', 'lindsey', 'tracey', 'seymour', 'scottie', 'eugenio',
                    'mohammed', 'sandy', 'valentin', 'chance', 'arnulfo', 'lucien', 'ferdinand', 'thad', 'ezra',
                    'sydney', 'aldo', 'rubin', 'royal', 'mitch', 'earle', 'abe', 'wyatt', 'marquis', 'lanny', 'kareem',
                    'jamar', 'boris', 'isiah', 'emile', 'elmo', 'aron', 'leopoldo', 'everette', 'josef', 'gail', 'eloy',
                    'dorian', 'rodrick', 'reinaldo', 'lucio', 'jerrod', 'weston', 'hershel', 'barton', 'parker',
                    'lemuel', 'lavern', 'burt', 'jules', 'gil', 'eliseo', 'ahmad', 'nigel', 'efren', 'antwan', 'alden',
                    'margarito', 'coleman', 'refugio', 'dino', 'osvaldo', 'les', 'deandre', 'normand', 'kieth', 'ivory',
                    'andrea', 'trey', 'norberto', 'napoleon', 'jerold', 'fritz', 'rosendo', 'milford', 'sang', 'deon',
                    'christoper', 'alfonzo', 'lyman', 'josiah', 'brant', 'wilton', 'rico', 'jamaal', 'dewitt', 'carol',
                    'brenton', 'yong', 'olin', 'foster', 'faustino', 'claudio', 'judson', 'gino', 'edgardo', 'berry',
                    'alec', 'tanner', 'jarred', 'donn', 'trinidad', 'tad', 'shirley', 'prince', 'porfirio', 'odis',
                    'maria', 'lenard', 'chauncey', 'chang', 'tod', 'mel', 'marcelo', 'kory', 'augustus', 'keven',
                    'hilario', 'bud', 'sal', 'rosario', 'orval', 'mauro', 'dannie', 'zachariah', 'olen', 'anibal',
                    'milo', 'jed', 'frances', 'thanh', 'dillon', 'amado', 'newton', 'connie', 'lenny', 'tory', 'richie',
                    'lupe', 'horacio', 'brice', 'mohamed', 'delmer', 'dario', 'reyes', 'dee', 'mac', 'jonah', 'jerrold',
                    'robt', 'hank', 'sung', 'rupert', 'rolland', 'kenton', 'damion', 'chi', 'antone', 'waldo',
                    'fredric', 'bradly', 'quinn', 'kip', 'burl', 'walker', 'tyree', 'jefferey', 'ahmed', 'willy',
                    'stanford', 'oren', 'noble', 'moshe', 'mikel', 'enoch', 'brendon', 'quintin', 'jamison',
                    'florencio', 'darrick', 'tobias', 'minh', 'hassan', 'giuseppe', 'demarcus', 'cletus', 'tyrell',
                    'lyndon', 'keenan', 'werner', 'theo', 'geraldo', 'lou', 'columbus', 'chet', 'bertram', 'markus',
                    'huey', 'hilton', 'dwain', 'donte', 'tyron', 'omer', 'isaias', 'hipolito', 'fermin', 'chung',
                    'adalberto', 'valentine', 'jamey', 'bo', 'barrett', 'whitney', 'teodoro', 'mckinley', 'maximo',
                    'garfield', 'sol', 'raleigh', 'lawerence', 'abram', 'rashad', 'king', 'emmitt', 'daron', 'chong',
                    'samual', 'paris', 'otha', 'miquel', 'lacy', 'eusebio', 'dong', 'domenic', 'darron', 'buster',
                    'antonia', 'wilber', 'renato', 'jc', 'hoyt', 'haywood', 'ezekiel', 'chas', 'florentino', 'elroy',
                    'clemente', 'arden', 'neville', 'kelley', 'edison', 'deshawn', 'carrol', 'shayne', 'nathanial',
                    'jordon', 'danilo', 'claud', 'val', 'sherwood', 'raymon', 'rayford', 'cristobal', 'ambrose',
                    'titus', 'hyman', 'felton', 'ezequiel', 'erasmo', 'stanton', 'lonny', 'len', 'ike', 'milan', 'lino',
                    'jarod', 'herb', 'andreas', 'walton', 'rhett', 'palmer', 'jude', 'douglass', 'cordell', 'oswaldo',
                    'ellsworth', 'virgilio', 'toney', 'nathanael', 'del', 'britt', 'benedict', 'mose', 'hong', 'leigh',
                    'johnson', 'isreal', 'gayle', 'garret', 'fausto', 'asa', 'arlen', 'zack', 'warner', 'modesto',
                    'francesco', 'manual', 'jae', 'gaylord', 'gaston', 'filiberto', 'deangelo', 'michale', 'granville',
                    'wes', 'malik', 'zackary', 'tuan', 'nicky', 'eldridge', 'cristopher', 'cortez', 'antione', 'malcom',
                    'long', 'korey', 'jospeh', 'colton', 'waylon', 'von', 'hosea', 'shad', 'santo', 'rudolf', 'rolf',
                    'rey', 'renaldo', 'marcellus', 'lucius', 'lesley', 'kristofer', 'boyce', 'benton', 'man', 'kasey',
                    'jewell', 'hayden', 'harland', 'arnoldo', 'rueben', 'leandro', 'kraig', 'jerrell', 'jeromy',
                    'hobert', 'cedrick', 'arlie', 'winford', 'wally', 'patricia', 'luigi', 'keneth', 'jacinto', 'graig',
                    'franklyn', 'edmundo', 'sid', 'porter', 'leif', 'lauren', 'jeramy', 'elisha', 'buck', 'willian',
                    'vincenzo', 'shon', 'michal', 'lynwood', 'lindsay', 'jewel', 'jere', 'hai', 'elden', 'dorsey',
                    'darell', 'broderick', 'alonso']
# initalize a column for dummy variable
data['dummy_firstname_recognized'] = 0
# then change dummy_firstname_recognized to 1 if firstname_alias_adj in recognized_names
data.ix[(data['firstname_alias_adj'].isin(recognized_names)), 'dummy_firstname_recognized'] = 1

################################################################################
# Similarity Matrix Creation
min_similarity = 0.87  # should be greater than the lowest cutoff used in script.
# do not include string pairs whose similarity is lower
# than min_similarity. to keep the file small. otherwise we have 30kx30k lines.

# comparing fullname_alias_adj
fullnames = data['fullname_alias_adj'].unique().tolist()
# define a similarity table two string columsn and one measure.
similarity_table = pandas.DataFrame(columns=['str1', 'str2', 'measure'])
# two for loops means we are selecting pairs. all possible pairs
'''
for i in range(0, len(fullnames)):
    print i, '/', len(fullnames) # to see the percentage process of code
    for j in range(0, i + 1):  #
        # get the ratio between two fullnames..
        s = SequenceMatcher(None, fullnames[i], fullnames[j]).ratio()
        # if the ratio is greater than minimum similarity..
        if s > min_similarity:
            # add two strings and their ratio to similarity table
            similarity_table.loc[len(similarity_table)] = fullnames[i], fullnames[j], s
# save similarity table to disk
similarity_table.to_csv('similarity.csv', encoding='utf8', index=False)
# load similarity from disk.
'''
similarity = pandas.read_csv('similarity.csv')

################################################################################
################################################################################
# Algorithms start
################################################################################
################################################################################

# for each cutoff. cutoff set will be [0.875, 0.900, 0.925, 0.9500]
for cutoffindex in range(875, 975, 25):
    cutoff = cutoffindex / 1000.0
    # we have a cutoff at this point.

    # create mid columns before filling them.
    for alg in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
        # (ci_ columsn are a way of keeping index of each row to be used in algorithms.)
        # when dealing with algorithm, it helps to know the row number, this column does not change at any moment, and only used to find the row number..
        data['ci_' + alg] = data.index.astype(int)
        # these columns (one for each algorithm and cutoff will keep the match id)
        data['mid_' + str(cutoff) + '_' + alg] = None

    # for each row in data...
    for i in data.index:
        print cutoff, i 
        # the code deals with i th row of data.
        # get the variables will be used in algorithms from row i.
        # so for example if shipname is seen below, it means it is the row i's shipname.
        row1name, portdep, shipname, datedep_range_first, datedep_range_last, \
        datedep_range_mean, last_name, initial, is_company, is_father, is_eldest, \
        is_brother, is_brothers, is_son, collaborators = GetVariablesFor(data, i)

        alg = 'A'
        similar_names, mark = FilterSimilarNames(similarity, row1name, cutoff, data)
        data.loc[i, 'mark'] = mark
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            filtered = FilterPortCollaboratorShipname(similar_names, collaborators, portdep, shipname, alg)
            filtered = FilterDateDep(filtered, datedep_range_first, datedep_range_last, datedep_range_mean)
            CreateMid(data, i, filtered, cutoff, alg)

        alg = 'B'
        # note that in this version similar_names_considering_fatherson is no more filters based on father - son
        similar_names_considering_fatherson = FilterFatherSonDummies(is_father, is_son, similar_names)
        
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            filtered = FilterPortCollaboratorShipname(similar_names_considering_fatherson, collaborators, portdep,
                                                      shipname, alg)
            filtered = FilterDateDep(filtered, datedep_range_first, datedep_range_last, datedep_range_mean)
            CreateMid(data, i, filtered, cutoff, alg)

        alg = 'C'
        if is_company == 0:
            filtered = FilterPortCollaboratorShipname(similar_names_considering_fatherson, collaborators, portdep,
                                                      shipname, alg)
            filtered = FilterDateDep(filtered, datedep_range_first, datedep_range_last, datedep_range_mean)
        else:
            filtered = similar_names_considering_fatherson.copy()
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            c_results = filtered.copy()
            CreateMid(data, i, c_results, cutoff, alg)

        alg = 'D'
        similar_names_considering_fatherson_and_recognized_names_and_firstnames = similar_names_considering_fatherson.append(
            FISOMF(data, last_name, initial)).drop_duplicates().reset_index(drop=True)
        if is_company == 0:
            filtered = FilterPortCollaboratorShipname(
                similar_names_considering_fatherson_and_recognized_names_and_firstnames, collaborators, portdep,
                shipname, alg)
            filtered = FilterDateDep(filtered, datedep_range_first, datedep_range_last, datedep_range_mean)
        else:
            filtered = similar_names_considering_fatherson_and_recognized_names_and_firstnames.copy()
        d_results = filtered.copy()
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            CreateMid(data, i, d_results, cutoff, alg)

        alg = 'E'
        matches_of_i_in_d = data[data['mid_' + str(cutoff) + '_D'] == data.loc[i, 'mid_' + str(cutoff) + '_D']]
        if len(matches_of_i_in_d['firstname_alias_adj'].unique().tolist()) > 1 and matches_of_i_in_d[
            'dummy_missing_firstname'].astype(int).sum() > 0:
            e_results = c_results.copy()
        else:
            e_results = d_results.copy()
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            CreateMid(data, i, e_results, cutoff, alg)

        alg = 'F'
        set_for_f = data[data['lastname_alias_adj'] == last_name][
            'firstname_alias_adj'].str.strip().unique().tolist()
        if len(set_for_f) == 2 and ('' in set_for_f):
            f_results = d_results.copy()
        else:
            f_results = e_results.copy()
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            CreateMid(data, i, f_results, cutoff, alg)
        
        alg = 'G'
        # get results in F, if row1 has no dummy, remove the guys with dummies from results.
        g_results = FilterKindredDummies(is_father, is_son, is_eldest, is_brother, is_brothers, f_results)
        if data.loc[i, 'mid_' + str(cutoff) + '_' + alg] == None:
            CreateMid(data, i, g_results, cutoff, alg)

data.to_csv('cutoff_algs' + str(cutoff) + '.csv', index=False, encoding='utf8')

data['dummy_captain'] = 0
# remove other columns except mid s generated by script
data = data[[k for k in data.columns if k.startswith('mid')] + ['mark', 'dummy_captain']]
# 1b reinserting unused columns to the generated ones.
data = pandas.concat([data, pandas.read_stata(original_data_path)], axis=1)

data.ix[((data['fullname_alias'] == data['captaina']) | (data['fullname_alias'] == data['captainb'])), 'dummy_captain'] = 1


# the part for assignin the longest name in the match group to fullname_dealias..
algs = ["A", 'B', 'C', 'D', 'E', 'F', 'G']
cutoffs = ['0.95', '0.925', '0.9', '0.875']
for cutoff in cutoffs:
    for alg in algs:
        data['fullname_dealias_' + str(cutoff) + '_' + alg] = None
        for i in data['mid_' + str(cutoff) + '_' + alg].unique().tolist():
            try:
                # fullname_dealias naming decision, longest or most frequent in match group.
                longest = max(data[data['mid_' + str(cutoff) + '_' + alg] == i]['fullname_alias_adj'].unique().tolist(),
                              key=len)
                mostfrequent = data[data['mid_'+str(cutoff)+'_'+alg] == i]['fullname_alias_adj'].value_counts().argmax()
                data.ix[data['mid_' + str(cutoff) + '_' + alg] == i, 'fullname_dealias_' + str(
                    cutoff) + '_' + alg] = mostfrequent
            except:
                pass
        for col in ['dummy_son','dummy_father','dummy_brother','dummy_brothers','dummy_eldest']:
            data.ix[data[col] == 1, 'fullname_dealias_' + str(
            cutoff) + '_' + alg] = data['fullname_dealias_' + str(cutoff) + '_' + alg] + ' "' + col.replace('dummy_','')+'"'

for j in range(0, len(cutoffs)):
    for i in range(0, len(algs) - 1):
        alg1 = algs[i]
        alg2 = algs[i + 1]
        alg = alg1 + alg2
        data['diff_' + cutoff + '_' + alg] = 0
        data.ix[
            data['mid_' + cutoff + '_' + alg1] != data['mid_' + cutoff + '_' + alg2], 'diff_' + cutoff + '_' + alg] = 1

    cutoff = cutoffs[j]
    if j != len(cutoffs) - 1:
        cutoff2 = cutoffs[j + 1]
    data['diff_F_' + cutoff + '_' + cutoff2] = 0
    data.ix[data['mid_' + cutoff + '_F'] != data['mid_' + cutoff2 + '_F'], 'diff_F_' + cutoff + '_' + cutoff2] = 1

data.to_csv('matches\\'+original_data_path + '_matches_v3.csv', encoding='utf8')

################################################################################
# Pathway 2
################################################################################
original_data_path = 'Data\\Orginal SV database_14043_merged_precleaned.dta'
data = pandas.read_csv('matches\\'+original_data_path + '_matches_v3.csv')
data['fullname_alias_adj'] = data['fullname_alias_adj'].str.replace('è', 'e').str.replace('é', 'e').str.replace('î','i').str.replace('ç', 'c').str.replace('ë', 'e').str.replace('É', 'E')
# do the same thing for collaborators.
for column in data.columns.tolist():
    if column.startswith('collaborator') and column.endswith('_adj'):
        data[column] = data[column].str.replace('è', 'e').str.replace('é', 'e').str.replace('î', 'i').str.replace('ç','c').str.replace('ë', 'e').str.replace('É', 'E')
data['rowid'] = data.index
data = data[data.dummy_company == 0]
similarity = pandas.read_csv('similarity.csv')
w_port, w_ship, w_captain, w_relation, w_collaborator = 2,2,1,6,3
prior_weight = pandas.DataFrame(columns=['x1','x2','x3','x4','x5','y'])
data['ss'] = 0
data['case'] = 0
data['ss_mid'] = 0
data['ss_fullname_dealias'] = None
for i in data.index:
    if len(str(data.loc[i,'firstname_alias_adj']).strip()) <  2 or str(data.loc[i,'firstname_alias_adj']).strip() ==  'nan':
        print i
        # get only pairs with similarity index greater than .9
        pm, m = FilterSimilarNames(similarity, data.loc[i,'fullname_alias_adj'], .9, data)
        pm = pm.append(data[data['lastname_alias_adj']==data.loc[i,'lastname_alias_adj']])
        pm = pm[pm['firstname_alias_adj'].astype(str) != str(data.loc[i,'firstname_alias_adj'])]
        pm = FilterDateDep(pm, data.loc[i,'datedep_range_first'], \
                           data.loc[i,'datedep_range_last'], data.loc[i,'datedep_range_mean'])
        pm = pm.reset_index(drop=True)
        ss_max = 0
        for j in pm.index:
            if data.loc[i, 'rowid'] != pm.loc[j,'rowid']:
                is_same_portdep = 1 if data.loc[i,'portdep'] == pm.loc[j,'portdep'] else 0
                is_same_shipname = 1 if data.loc[i,'shipname'] == pm.loc[j,'shipname'] else 0
                is_same_captaina = 1 if data.loc[i,'captaina'] == pm.loc[j,'captaina'] else 0
                is_same_relationship_dummy = 1 
                for col in ['dummy_son','dummy_father','dummy_brother','dummy_brothers','dummy_eldest']:
                    if data.loc[i,col] != pm.loc[j,col]:
                        is_same_relationship_dummy = 0
                no_of_common_collaborator = len(list(set(GetCollaboratorsOfRow(i, data)).intersection(set(GetCollaboratorsOfRow(j, pm)))))

                ss = is_same_portdep * w_port + is_same_shipname * w_ship + is_same_captaina * w_captain\
                     + (is_same_portdep + is_same_shipname + is_same_captaina) * is_same_relationship_dummy * w_relation\
                     + no_of_common_collaborator * w_collaborator
                if ss > ss_max:
                    ss_max = ss
                    data.loc[i, 'ss'] = ss_max
                    data.loc[i, 'ss_most_similar'] = pm.loc[j,'rowid']
                    data.loc[i, 'ss_most_similart_fullname_dealias'] = pm.loc[j,'fullname_alias_adj']
    
                if data.loc[pm.loc[j,'rowid'], 'ss_mid'] == 0:
                    if data.loc[i,'ss_mid'] != 0:
                        data.loc[pm.loc[j,'rowid'], 'ss_mid'] = data.loc[i,'ss_mid']
                    else:
                        data.loc[pm.loc[j,'rowid'], 'ss_mid'] = i
    
            
        if data.loc[i,'ss_mid'] != data.loc[i,'mid_0.9_F'] and data.loc[i,'mid_0.9_F'] != data.loc[data.loc[i,'ss_mid'],'mid_0.9_F']:
            data.loc[i, 'case'] = 1
data['ss'] = data['ss'].astype(float) / 44.0
data.to_csv('check.csv')

'''
pathway 2: supersimilarity index		
starting point is fullname_alias_adj		
1) exercise only done for pairs with similarity index = > .9		
2) exclude companies from entire exercise		
3) yearrange is still necessary condition		
4) compute supersimilarity index on the basis of:		
	1) portdep	
	2) shipname	
	3) collaborators (number of common collaborators)	
	4) captaina	
	5) relationship dummies (father, brother, son, etc.)	
'''
