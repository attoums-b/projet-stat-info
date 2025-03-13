import sqlite3
import csv




# connexion à la bd 

bd = sqlite3.connect("bd_incendies") 
curseur = bd.cursor() #curseur permettant d'executer les requetes SQL


"""
    LIENS ENTRE INCENDIES GEOGRAPHIE ET METEO 
        INCENDIE ET COMMUNES (GEOGRAPHIE)
            -un incendie peut avoir lieu dans une et une seule commune (car les données incendies ne peuvent pas 
    être exactement les memes pour deux communes différentes  )
            -une commune peut avoir un ou plusieurs incendies 

        COMMUNES ET METEO
            -une meteo en particulier est associée à une et une seule commune dans la base 
            -une commune (ou lieu géographique) peut avoir une ou plusieurs meteos (car il peut faire chaud ou
            faire froid dans cette commune)


"""
#création de la table Communes 
curseur.execute("""
CREATE TABLE IF NOT EXISTS  Communes(
                code_INSEE TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                altitude_med INTEGER               

);

""")

#création de la table Meteo 
curseur.execute("""
CREATE TABLE IF NOT EXISTS Meteo(
                id_meteo INTEGER PRIMARY KEY AUTOINCREMENT,
                code_INSEE TEXT ,
                RR_med REAL,
                NBJRR1_med REAL,
                NBJRR5_med REAL,
                NBJRR10_med REAL,
                Tmin_med REAL,
                Tmax_med REAL,
                Tens_vap_med REAL,
                Force_vent_med REAL,
                Insolation_med INTEGER,
                Rayonnement_med INTEGER,
                FOREIGN KEY(code_INSEE) REFERENCES Communes(code_INSEE)
                
                
);
""")



#creation de la table Incendies 
curseur.execute("""
CREATE TABLE IF NOT EXISTS Incendies(
                id_incendie INTEGER PRIMARY KEY AUTOINCREMENT,
                commune TEXT,
                code_INSEE TEXT ,
                surface_parcourue_m2 INTEGER,
                annee INTEGER,
                jour INTEGER,
                mois TEXT,
                heure INTEGER,
                nature_inc_prim TEXT,
                nature_inc_sec TEXT,
                FOREIGN KEY (code_INSEE) REFERENCES Communes(code_INSEE)
                
                
                

 );

""")



#gérer les valeurs manquantes (NA):
def clean_value(value):
    if value == 'NA':
        #on retourne None pour que le SQL l'interprete comme NULL 
        return None
    else:
        return value

#on importe les fichiers csv et on insère les données dans notre base 

#importation des données géographiques *


with open('donnees_geo.csv', mode="r",encoding="utf-8") as g:
    lecteur = csv.DictReader(g)


    for row in lecteur:
        
       
        
        curseur.execute("INSERT INTO Communes(code_INSEE,latitude ,longitude ,altitude_med) VALUES(?,?,?,?)",(
        row['code_INSEE'],
        row['latitude'] ,
        row['longitude'] ,
        row['altitude_med']
                
                ))
       
                
#importer la base de données météo

with open('donnees_meteo.csv', mode="r",encoding="utf-8") as m:
    lecteur = csv.DictReader(m)

    for row in lecteur:
        

        curseur.execute("INSERT INTO Meteo(code_INSEE,RR_med,NBJRR1_med ,NBJRR5_med ,NBJRR10_med ,Tmin_med ,Tmax_med ,Tens_vap_med ,Force_vent_med "
        ",Insolation_med,Rayonnement_med) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(
            row['Code_INSEE'],
            row['RR_med'],
            row['NBJRR1_med'] ,
            row['NBJRR5_med'] ,
            row['NBJRR10_med'] ,
            row['Tmin_med'] ,
            row['Tmax_med'] ,
            row['Tens_vap_med'] ,
            row['Force_vent_med'] ,
            clean_value(row['Insolation_med']),
            clean_value(row['Rayonnement_med'] )
            ))



#importation de la base de données incendies 


with open('donnees_incendies.csv',mode="r",encoding="utf-8") as i : #l'encodage permet de bien afficher les caractères comme dans le fichier CSV
    lecteur = csv.DictReader(i)
    
    for row in lecteur:
       
       curseur.execute("INSERT INTO Incendies(commune,code_INSEE,surface_parcourue_m2,annee,mois,jour,heure,"
        "nature_inc_prim,nature_inc_sec) VALUES(?,?,?,?,?,?,?,?,?) ",(
            row['commune'],
            row['code_INSEE'],
            row['surface_parcourue_m2'],
            row['annee'],
            row['mois'],
            row['jour'],
            row['heure'],
            row['nature_inc_prim'],
            clean_value(row['nature_inc_sec'])
            
            ))
        

#on envoie l'ensemble de nos requêtes à la BD
bd.commit()
bd.close()








