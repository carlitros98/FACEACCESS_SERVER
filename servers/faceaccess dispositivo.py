import random
import time
import psycopg2
import json
import os
import threading
import requests
import sys
import signal
import liquidcrystal_i2c
import time
from datetime import datetime
from dateutil import parser
from time import sleep, mktime

from paho.mqtt import client as mqtt_client

lock = threading.Lock()

#Establecemos la conexión con la BBDD
conn = psycopg2.connect(
   database="server_db", user='pi', password='toor', host='localhost', port= '5432'
)

message_alert_token = 0

broker = str(sys.argv[2]) #'192.168.1.90'
port = int(sys.argv[4]) #1883

text_ip = "IP: " + broker

lcd = liquidcrystal_i2c.LiquidCrystal_I2C(0x27, 1, numlines=4)
time.sleep(1)
lcd.clear()
lcd.printline(0,"**FaceAccess brain**")
lcd.printline(2,"Loading....")
lcd.printline(3,text_ip)




#Topics para la obtención de la salvaguarda de la base de datos

topic_pub_db = 'receiveDatabase'
topic_sub_db = 'getDatabase'


#Topics para la obtención/envío de información relacionada con el acceso/registro de clientes

topic_pub_cl = 'receiveClient'
topic_sub_cl = 'getClient'


#Envío del aforo actualizado a los empleados

topic_pub_em = 'receiveServerEmpleado'

#Información relacionada con las notificaciones push

url = 'https://onesignal.com/api/v1/notifications'

token_server = 'b150d07e-3f0a-42bb-a7ee-6d9c5d2650e2'


#Dar de alta a cliente 1 en MQTT

def connect_mqtt(client_id, thread):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker! - App dispositivo")
            print("Subscriber: " + thread)
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


#Dar de alta a cliente 2 en MQTT
    
def connect_mqtt2(client_id, thread):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker! - App dispositivo")
            print("Publisher: " + thread)
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


#Handler para responder a la información llegada desde los tópicos
    
def subscribe(client: mqtt_client, client2, topic_pub, topic_sub):
    def on_message(client, userdata, msg):
        print('Respondiendo a la información obtenida desde el topico ' + str(msg.topic))
        resultado = getFunction(msg.payload.decode(), client2, topic_pub)
        time.sleep(1)
        publish(client2, str(resultado),topic_pub)
    
    client.subscribe(topic_sub)
    client.on_message = on_message
    
   
#Publicación de mensajes MQTT

def publish(client, msg, topic):
    time.sleep(1)
    client.publish(topic, msg)
 

#Comprueba si el formato de los mensajes es correcto
    
def is_json(myjson):
    try:
        json.loads(myjson)
    except:
        return False
    return True

#Dependiendo del mensaje enviado, se responderá con una función u otra

def getFunction(msg, client2, topic_pub):
    if is_json(msg) == True:

        try:
            y = json.loads(msg)
            z = y['function']
            sender = y['sender']
            datos = y['data']
        
            if z == "getDatabase" and topic_pub == topic_pub_db: 
                return getDatabase(datos, sender)
        
            if z == "registerClient" and topic_pub == topic_pub_cl: 
                return registerClient(datos, sender, client2)
            
            if z == "requestAccess" and topic_pub == topic_pub_cl: 
                return requestAccess(datos, sender, client2)
                
            if z == "helpClient" and topic_pub == topic_pub_cl: 
                return helpClient(datos, sender)
            
            return {"function" : "getAlta", "status" : "KO", "message" : "Formato invalido mensajes", "data" : "", "receiver" : sender}  

        
        except:
            return {"function" : "getAlta", "status" : "KO", "message" : "Error en el proceso", "data" : "", "receiver" : "null"}  
    
    else:
        return "null" 


#Implementación de las funciones

def getDatabase(datos, sender):
    try:
        id_actual = int(datos['id'])
              
        clientes = getClientes(id_actual) 
                
        return {"function" : "getDatabase", "status" : "OK", "message" : "", "data" : clientes, "receiver" : sender}   

        
    except Exception as e:
        return {"function" : "getDatabase", "status" : "KO", "message" : "Error en el proceso", "data" : "", "receiver" : sender}   


def helpClient(datos, sender):
    cuerpo = "Han presionado el botón de ayuda"
    headings = "Dispositivo con identificador " + sender
    notificacion(cuerpo, headings)
    return {"function" : "helpClient", "status" : "OK", "message" : "", "data" : "", "receiver" : sender}   


def registerClient(datos, sender, client2):
    try:
        certificate_id = datos['certificate_id']
        nombre = datos['nombre']
        apellidos = datos['apellidos']
        fecha = datos['fecha']
        puntos = datos['puntos']
        photo = datos['photo']
        
        exist = existCertificate(certificate_id)
        print('pregreg1')
        
        if exist == 1: #exist
            cur = conn.cursor()
            cur.execute("""UPDATE cliente SET puntos = %s WHERE certificate_id = %s""", [puntos, certificate_id])
            conn.commit()   
            clientState = clientStateFunc(certificate_id)
                        
            if clientState == 0:
                        
                #fase solicitud acceso

                disponible = existAforo()
        
                if disponible == 1:
                    cur = conn.cursor()
                    cur.execute("""UPDATE cliente SET presente = 1 WHERE certificate_id = %s""", [certificate_id])
                    conn.commit()

                    incrementAforo(client2, (nombre + " " + apellidos), certificate_id, "in", photo)
                    return sendBroadcastUpdate(sender, certificate_id, client2, "", "OK")
        
                else:
                    return sendBroadcastUpdate(sender, certificate_id, client2, "Cliente registrado, pero el aforo está lleno", "OKO") 
                 
            else:
                cur = conn.cursor()
                cur.execute("""UPDATE cliente SET presente = 0 WHERE certificate_id = %s""", [certificate_id])
                conn.commit()
            
                decrementAforo(client2, (nombre + " " + apellidos), certificate_id, "out", photo)
                return sendBroadcastUpdate(sender, certificate_id, client2, "", "OK")
            

        else: # no exist
            if clientIsAdult(str(fecha)) == False:
                cuerpo = "Una persona no autorizada está tratando de acceder al establecimiento (menor de edad)"
                headings = "ALERTA"
                notificacion(cuerpo, headings)
                return {"function" : "forbiddenClient", "status" : "KO", "message" : "No pueden acceder al establecimiento personas menores de edad", "data" : "", "receiver" : sender} 

            if clientInRegister(str(certificate_id)) == True:
                cuerpo = "Una persona no autorizada está tratando de acceder al establecimiento (registro oficial)"
                headings = "ALERTA"
                notificacion(cuerpo, headings)
                return {"function" : "forbiddenClient", "status" : "KO", "message" : "Prohibido el acceso al establecimiento", "data" : "", "receiver" : sender}  
                
            cur = conn.cursor()
            cur.execute('INSERT INTO cliente (certificate_id, nombre, apellidos,fecha, puntos, presente, photo) VALUES (%s, %s, %s, %s, %s, %s, %s)', (certificate_id, nombre, apellidos, fecha, puntos, "0", photo))
            conn.commit()
            
            cur = conn.cursor()
            cur.execute("""SELECT id from cliente WHERE certificate_id = %s""", [certificate_id])
            
            results = cur.fetchone()
            
            id_nuevo = results[0]
            #fase solicitud acceso

            disponible = existAforo()
            
            if disponible == 1:
                cur = conn.cursor()
                cur.execute("""UPDATE cliente SET presente = 1 WHERE certificate_id = %s""", [certificate_id])
                conn.commit()
            
                incrementAforo(client2, (nombre + " " + apellidos), certificate_id, "in", photo)
                return sendBroadcastUpdate(sender, certificate_id, client2, "", "OK")
        
            else:
                return sendBroadcastUpdate(sender, certificate_id, client2, "Cliente registrado, pero el aforo está lleno", "OKO")   

        
    except Exception as e:

        return {"function" : "registerClient", "status" : "KO", "message" : "Error en el proceso" + str(e), "data" : "", "receiver" : sender}   
 
    


def sendBroadcastUpdate(sender, certificate_id, client2, msg, status):
    cur = conn.cursor()
    cur.execute("""SELECT * from cliente WHERE certificate_id = %s""",[certificate_id])
    results = cur.fetchone()
    id_c = int(results[0])
    nombre = results[2]
    apellidos = results[3]
    fecha = results[4]
    points = results[5]
    client = []
    
    client.append({"id" : id_c, "certificate_id" : certificate_id, "nombre" : nombre, "apellidos" : apellidos, "fecha" : fecha, "puntos" : points})
    

    msg = {"function" : "updateClient", "status" : status, "message" : msg, "data" : client, "receiver" : sender}
    return msg


def requestAccess(datos, sender, client2):

    try:
        certificate_id = datos['certificate_id']
        clientState = clientStateFunc(certificate_id)
        print('pre_Re')
        cur = conn.cursor()
        cur.execute("""SELECT nombre, apellidos, photo from cliente WHERE certificate_id = %s""",[certificate_id])
        results1= cur.fetchone()
        
        name = results1[0]
        apellidos = results1[1]
        photo = results1[2]
        
        print('post_Re')
        if clientState == 0: #entrada cliente
            disponible = existAforo()
            if disponible == 1:
                cur = conn.cursor()
                cur.execute("""UPDATE cliente SET presente = 1 WHERE certificate_id = %s""", [certificate_id])
                conn.commit()
                incrementAforo(client2, (name + " " + apellidos), certificate_id, "in", photo)
                return {"function" : "requestAccess", "status" : "OK", "message" : "", "data" : "", "receiver" : sender} 
        
            else:
                return {"function" : "requestAccess", "status" : "KO", "message" : "Aforo completo", "data" : "", "receiver" : sender}  
                
                        
        else:  #salida cliente
            cur = conn.cursor()
            cur.execute("""UPDATE cliente set presente = 0 WHERE certificate_id = %s""", [certificate_id])
            conn.commit()  
             
            decrementAforo(client2, (name + " " + apellidos), certificate_id, "out", photo)    
            return {"function" : "requestAccess", "status" : "OK", "message" : "", "data" : "", "receiver" : sender} 
  
        
    except Exception as e:

        return {"function" : "requestAccess", "status" : "KO", "message" : "Error en el proceso", "data" : "", "receiver" : sender}   
 
    
#Consultas a la BBDD

def printAforo(actual, maximo):
    global text_ip
    
    
    texto = "AFORO:  " + actual + "/" + maximo
    
    lock.acquire()
    #lcd.clear()
    #lcd.printline(0,"**FaceAccess brain**")
    lcd.printline(2,"                    ")
    lcd.printline(2,texto)
    #lcd.printline(3,text_ip)
    
    lock.release()
        

def clientIsAdult(birthDate):
    
    cur = conn.cursor()

    cur.execute("""SELECT registro from establecimiento""", [])
    
    results_a = cur.fetchone()
    
    
    if results_a[0] == 1:

        today = datetime.today()
        birth = parser.parse(birthDate)
        age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))

        if age < 18:
            return False

    return True

def clientInRegister(certificate_id):
    
    cur = conn.cursor()

    cur.execute("""SELECT menores from establecimiento""", [])
    
    results_a = cur.fetchone()
    
    if results_a[0] == 1:

        cur2 = conn.cursor()

        cur2.execute("""SELECT count(*) from registro WHERE certificate_id = %s""", [certificate_id])

        results_b = cur2.fetchone()
    	
        if results_b[0] == 1:
    	    return True  
    

    return False

def incrementAforo(client2, name, certificate_id, state, photo):

    cur = conn.cursor()
    cur.execute("""SELECT aforo_actual, aforo_maximo from establecimiento WHERE id_establecimiento = 1""",[])
    
    results = cur.fetchone()  

    new_aforo = int(results[0]) + 1
    cur = conn.cursor()
    cur.execute('UPDATE establecimiento set aforo_actual = %s WHERE id_establecimiento = 1', [str(new_aforo)])
    conn.commit()

    sendAforoEmpleado(client2, name, certificate_id, state, photo)
    printAforo(str(new_aforo), str(results[1]))

def decrementAforo(client2, name, certificate_id, state, photo):

    cur = conn.cursor()

    cur.execute("""SELECT aforo_actual, aforo_maximo from establecimiento WHERE id_establecimiento = 1""",)
    
    results = cur.fetchone()  
    
    new_aforo = int(results[0]) - 1
    
    cur = conn.cursor()
    cur.execute('UPDATE establecimiento set aforo_actual = %s WHERE id_establecimiento = 1', [str(new_aforo)])
    conn.commit()
    
    sendAforoEmpleado(client2, name, certificate_id, state, photo)
    printAforo(str(new_aforo), str(results[1]))


def sendAforoEmpleado(client2, name, certificate_id, state, photo):
    global message_alert_token
    cur = conn.cursor()
    cur.execute("""SELECT aforo_actual, aforo_maximo from establecimiento WHERE id_establecimiento = 1""",)
    results = cur.fetchone()
    
    actual = int(results[0])
    maximo = int(results[1])

    porcentaje = float(actual/maximo)
    

    datos = {"aforo_actual" : actual, "aforo_maximo" : maximo, "nombre" : name, "certificate" : certificate_id, "action" : state, "photo" : photo}
    

    msg = {"function" : "updateAforo", "status" : "OK", "message" : "", "data" : datos, "receiver" : "broadcast"}  
    
    
    if porcentaje >= 0.75 and message_alert_token == 0:
        cuerpo = "El aforo se está agotando"
        headings = "AVISO"
        notificacion(cuerpo, headings)

        message_alert_token = 1
            
    if porcentaje < 0.5:
        message_alert_token = 0
            
    
    publish(client2, str(msg), topic_pub_em)    
    

def clientStateFunc(certificate_id):
    
    cur = conn.cursor()

    cur.execute("""SELECT presente from cliente WHERE certificate_id = %s""", [certificate_id])

    results = cur.fetchone()  
    

    return results[0]
    
    
def getClientes(id_actual):

    clientes_list = []

    cur = conn.cursor()

    cur.execute("""SELECT * from cliente WHERE id > %s""", [str(id_actual)])
    
    results = cur.fetchall()
   
    
    for r in results:
        individual = {"id" : None, "certificate_id" : None, "nombre" : None, "apellidos" : None, "fecha" : None, "puntos" : None}
        
        individual['id'] = r[0]
        individual['certificate_id'] = r[1]
        individual['nombre'] = r[2]
        individual['apellidos'] = r[3]
        individual['fecha'] = r[4]
        individual['puntos'] = r[5]
        clientes_list.append(individual)
    
    return clientes_list

def existAforo():
    cur = conn.cursor()
    cur.execute("""SELECT aforo_actual, aforo_maximo from establecimiento WHERE id_establecimiento = 1""",)
    results = cur.fetchone()
    
    actual = results[0]
    maximo = results[1]
    

    if actual < maximo:
        return 1
        
    else:
        return 0

def existCertificate(cert_id):
    cur = conn.cursor()
    cur.execute("""SELECT count(*) from cliente WHERE certificate_id = %s""",[cert_id])
    results = cur.fetchone()
    
    return results[0]

#--------fin consultas BBDD---------------


#Función para las notificaciones push


def notificacion(cuerpo, headings):
    token_list = getEmployeeToken()
    body = {
       "app_id" : token_server,
       "include_player_ids" : token_list,
       "headings" : {"en" : headings},
       "contents" : {"en" : cuerpo}
    }
    
    headers = {"Content-Type" : "application/json"}
    
    requests.post(url, data = json.dumps(body), headers = headers)
    

#Obtener token del dispositivo del empleado

def getEmployeeToken():
    token_list = []
    cur = conn.cursor()
    cur.execute("""SELECT id_onesignal from empleado""",)
    results = cur.fetchall()
    
    for row in results:
        token_list.append(str(row[0]))
    
    return token_list

#Dar de alta a los clientes MQTT en los diferentes threads

def alta_conexion(topic_pub, topic_sub, thread):
    client_sub = connect_mqtt(thread + "sub (device)", thread)
    client_pub = connect_mqtt2(thread + "pub (device)", thread)
    subscribe(client_sub, client_pub, topic_pub, topic_sub)
    client_pub.loop_start()
    client_sub.loop_forever()
    

def initial_message():
    print('\nSe cargarán las dependencias con los tópicos de los dispositivos de control....')
    
def printHour():
    dti = mktime(datetime.now().timetuple())
    
    
    while 1:
        ndti = mktime(datetime.now().timetuple())
        
        if dti < ndti:
            dti = ndti
            
            lock.acquire()
            lcd.printline(1,"HORA: "+datetime.now().strftime('%H:%M:%S'))
            lock.release()
            
            sleep(0.95)
        else:
            sleep(0.01) 

    
def run():
    cur = conn.cursor()
    cur.execute("""SELECT aforo_actual, aforo_maximo from establecimiento WHERE id_establecimiento = 1""",[])
    
    results = cur.fetchone() 
    printAforo(str(results[0]), str(results[1]))
    
    initial_message()
    
    threads = []
    
    thread_database = threading.Thread(target = alta_conexion, args = (topic_pub_db, topic_sub_db, "thread 1 (device)"))
    thread_client = threading.Thread(target = alta_conexion, args = (topic_pub_cl, topic_sub_cl, "thread 2 (device)"))
    thread_hour = threading.Thread(target = printHour)
    threads.append(thread_database)
    threads.append(thread_client)
    threads.append(thread_hour)


    thread_database.start()
    print("\n")
    time.sleep(5)
    thread_client.start()
    print("\n")
    time.sleep(10)
    thread_hour.start()
    print("\n")
    
if __name__ == '__main__':
    run()


