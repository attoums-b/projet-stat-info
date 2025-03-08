import sqlite3
import csv



# connexion à la bd 

bd = sqlite3.connect("bd_incendies") 
curseur = bd.cursor() #curseur permettant d'executer les requetes SQL


"""
    LIENS ENTRE INCENDIES GEOGRAPHIE ET METEO 
        INCENDIE ET GEOGRAPHIE
            -un incendie peut avoir lieu dans une et une seule commune donc 1 lieu géographique
            -des données géographiques sont associées à aucun ou plusieurs incendies 

        GEOGRAPHIE ET METEO
            -une meteo en particulier est associée à un lieu géographique 
            -un lieu géographique peut avoir un ou plusieurs metos 


"""

curseur.execute("""

CREATE TABLE IF NOT EXISTS Geographie(
                code_INSEE TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                altitude_med INTEGER
                
                

);

""")


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
                FOREIGN KEY(code_INSEE) REFERENCES Geographie(code_INSEE)
                
                
);
""")




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
                nature_inc_prim TEXT CHECK(nature_inc_prim IN("Involontaire","Accidentelle","Malveillance")),
                nature_inc_sec TEXT CHECK(nature_inc_sec IN("Travaux","Particulier","NA")),
                FOREIGN KEY (code_INSEE) REFERENCES Geographie(code_INSEE)
                
                
                

 );

""")

bd.commit()



#on importe les fichiers csv et on insère les données dans notre base 

#importation des données géographiques 



#importer la base de données météo

with open('donnees_meteo.csv', mode="r") as m:
    lecteur = csv.DictReader(m)

    next(lecteur)

    for row in lecteur:
        code_INSEE,RR_med,NBJRR1_med ,NBJRR5_med ,NBJRR10_med ,Tmin_med ,Tmax_med ,Tens_vap_med ,Force_vent_med ,Insolation_med,Rayonnement_med = row
        curseur.execute("INSERT INTO Meteo(code_INSEE,RR_med,NBJRR1_med ,NBJRR5_med ,NBJRR10_med ,Tmin_med ,Tmax_med ,Tens_vap_med ,Force_vent_med ,Insolation_med,Rayonnement_med) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (code_INSEE,RR_med,NBJRR1_med ,NBJRR5_med ,NBJRR10_med ,Tmin_med ,Tmax_med ,Tens_vap_med ,Force_vent_med ,Insolation_med,Rayonnement_med ))


#importation de la base de données incendies 

with open('donnees_incendies.csv',mode="r") as i :
    lecteur = csv.DictReader(i)

    next(lecteur)

    for row in lecteur:
        commune,code_INSEE,surface_parcourue_m2,annee,mois,jour,heure,nature_inc_prim,nature_inc_sec = row
        curseur.execute("INSERT INTO Incendies(commune,code_INSEE,surface_parcourue_m2,annee,mois,jour,heure,nature_inc_prim,nature_inc_sec) VALUES(?,?,?,?,?,?,?,?,?) ",
                        (commune,code_INSEE,surface_parcourue_m2,annee,mois,jour,heure,nature_inc_prim,nature_inc_sec))

bd.commit()
bd.close()






