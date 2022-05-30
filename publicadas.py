import requests as req
import codecs
import json
import pandas as pd
import datetime
import time
import sys

def proceso():
    now = datetime.datetime.now()
    #now
    #start = datetime.datetime(2019,1,1)
    start = datetime.datetime(2022,5,26)
    salida = []
    avance = start
    while avance < now:
        print(avance.strftime("%d%m%y"))
        fecha = avance.strftime("%d%m") + "20" + avance.strftime("%y")
        
        url = f"http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha={fecha}&estado=publicada&ticket=BC2B1276-7EF0-48FA-9EA8-888BFD8D11FE"
        #print(url)
        response = req.get(url)
        decoded_data=codecs.decode(response.content, 'utf-8-sig')
        d = json.loads(decoded_data)
        try:
            
            df = pd.DataFrame(d["Listado"])
            df["FechaPublicada"] = avance
            salida.append(df)
            #print(f"Exito en {fecha}")
        except:
            print(f"error en {fecha}")
        time.sleep(2)
        avance += datetime.timedelta(days = 1)
    final = pd.concat(salida)
    #final  

    final["Link"] = final["CodigoExterno"].apply(lambda x: f"http://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={x}")
    final.to_excel("licitaciones_publicadas_2019.xlsx", index=False)
    return None

def transformar_fecha(texto):
    try:
        fecha = datetime.datetime.strptime(texto,"%Y-%m-%dT%H:%M:%S")
    except:
        fecha = None
    return fecha

def GetFecha(texto):
    try:
        return datetime.datetime.strptime(texto,"%Y-%m-%d %H:%M:%S")
    except:
        try:
            return datetime.datetime.strptime(texto.split(" "),"%Y-%m-%d")
        except:
            return texto

def ChangeT(texto):
    try:
        return texto.replace("T"," ")
    except:
        return texto


def proceso2():
    print("Comenzamos publicadas...")
    ref = pd.read_excel("https://github.com/Sud-Austral/ACTION_LICITACION/raw/main/licitaciones_publicadas_2019.xlsx")
    
    now = datetime.datetime.now()
    #now
    start = ref["FechaPublicada"].max()
    salida = []
    avance = start
    while avance < now:
        print(avance.strftime("%d%m%y"))
        fecha = avance.strftime("%d%m") + "20" + avance.strftime("%y")        
        url = f"http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha={fecha}&estado=publicada&ticket=BC2B1276-7EF0-48FA-9EA8-888BFD8D11FE"
        #print(url)
        flag = True
        while flag:
            response = req.get(url)
            decoded_data=codecs.decode(response.content, 'utf-8-sig')
            d = json.loads(decoded_data)
            print(d)
            try:            
                df = pd.DataFrame(d["Listado"])
                df["FechaPublicada"] = avance
                salida.append(df)
                flag = False
                #print(f"Exito en {fecha}")
            except:
                print(f"error en {fecha}")
                error = sys.exc_info()[1]
                print(error)
        time.sleep(2)
        avance += datetime.timedelta(days = 1)
    final = pd.concat(salida)
    final["Link"] = final["CodigoExterno"].apply(lambda x: f"http://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={x}")

    tabla_final = pd.concat([ref,final])
    
    tabla_final = tabla_final.drop_duplicates()

    #tabla_final["FechaCierre"]=tabla_final["FechaCierre"].apply(transformar_fecha)
    tabla_final = tabla_final.dropna(subset=['FechaCierre'])
    tabla_final["FechaCierre"] = tabla_final["FechaCierre"].apply(ChangeT)
    tabla_final["FechaCierre"] = tabla_final["FechaCierre"].apply(GetFecha)
    tabla_final = tabla_final.drop_duplicates(subset=['CodigoExterno'])
    tabla_final.to_excel("licitaciones_publicadas_2019.xlsx", index=False)
    print("*****************************************")
    print("*****************************************")
    print("*****************************************")
    print("*****************************************")
    print("*****************************************")
    print("Fin ")
    return

if __name__ == '__main__':
    print("Publicadas...")
    proceso2()