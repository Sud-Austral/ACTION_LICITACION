import requests as req
import codecs
import json
import pandas as pd
import datetime
import time

def proceso():
    now = datetime.datetime.now()
    #now
    start = datetime.datetime(2019,1,1)
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

if __name__ == '__main__':
    print("Comenzo...")
    proceso()