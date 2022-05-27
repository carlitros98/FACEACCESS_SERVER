import random
import time
import psycopg2
import json
import os
import threading
import sys
import signal

from paho.mqtt import client as mqtt_client

#Establecemos la conexión con la BBDD
conn = psycopg2.connect(
   database="server_db", user='pi', password='toor', host='localhost', port= '5432'
)


broker = str(sys.argv[2]) #'192.168.1.90'
port = int(sys.argv[4]) #1883


#Topics para el alta de cuentas en la aplicación empleado

topic_pub_reg = 'receiveAltaEmpleado'
topic_sub_reg = 'altaEmpleado'


#Topics para el login de cuentas en la aplicación empleado

topic_pub_log = 'receiveLoginEmpleado'
topic_sub_log = 'loginEmpleado'


#Dar de alta a cliente 1 en MQTT

def connect_mqtt(client_id, thread):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker! - App empleado")
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
            print("Connected to MQTT Broker! - App empleado")
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
        resultado = getFunction(msg.payload.decode(), topic_pub)
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

def getFunction(msg, topic_pub):

    if is_json(msg) == True:

        try:
            y = json.loads(msg)
            z = y['function']
            sender = y['sender']
            datos = y['data']
        
            if z == "altaEmpleado" and topic_pub == topic_pub_reg:
                return getAlta(datos, sender)
        
            if z == "loginEmpleado" and topic_pub == topic_pub_log:
                return getLogin(datos, sender)
   
            
            return {"function" : "altaEmpleado", "status" : "KO", "message" : "Formato invalido mensajes", "data" : "", "receiver" : sender}  

        
        except:
            return {"function" : "altaEmpleado", "status" : "KO", "message" : "Error en el proceso", "data" : "", "receiver" : "null"}  
    
    else:
        return "null" 


#Implementación de las funciones

def getAlta(datos, sender):
    
    try:

        nick = datos['nick']
        pwd = datos['pwd']
        name = datos['name']
        surname = datos['surname']
        cert = datos['id_cert']
        onesignal = datos['id_onesignal']  
      
        nick_ok = existNick(nick) 
        cert_ok = existCert(cert)
        aforo_actual = getActual()
        aforo_max = getMax()
        jsonRet = {"aforo_actual"  : aforo_actual, "aforo_max" :  aforo_max }

        if nick_ok == 0 and nick != "broadcast":
            if cert_ok == 1:
                cur = conn.cursor()
                cur.execute('INSERT INTO empleado (nickname, pwd, nombre, apellidos, id_cert, id_onesignal) VALUES (%s, %s, %s, %s, %s, %s)', (nick, pwd, name, surname, cert, onesignal))
                conn.commit()
                
                auxcur = conn.cursor()
                auxcur.execute("""UPDATE certificados SET uso = 1 WHERE id_certificado = %s""",[cert])
                
                conn.commit()
                
                return {"function" : "altaEmpleado", "status" : "OK", "message" : "", "data" : jsonRet, "receiver" : sender}
                
            else:
                return {"function" : "altaEmpleado", "status" : "KO", "message" : "Certificado no disponible", "data" : "", "receiver" : sender}

        else:
            return {"function" : "altaEmpleado", "status" : "KO", "message" : "Nick no disponible", "data" : "", "receiver" : sender}  
        
    except Exception as e:

        return {"function" : "altaEmpleado", "status" : "KO", "message" : "Error en el proceso " + str(e), "data" : "", "receiver" : sender}   
    

def getLogin(datos, sender):

    try:
        nick = datos['nick']
        pwd = datos['pwd']      
        log_ok = existLogin(nick,pwd) 
        aforo_actual = getActual()
        aforo_max = getMax()
        jsonRet = {"aforo_actual"  : aforo_actual, "aforo_max" :  aforo_max }
        
        if log_ok == 1:

            return {"function" : "loginEmpleado", "status" : "OK", "message" : "", "data" : jsonRet, "receiver" : sender}
                
        else:
            return {"function" : "loginEmpleado", "status" : "KO", "message" : "Credenciales inválidas", "data" : "", "receiver" : sender}   
        

    except Exception as e:
        return {"function" : "loginEmpleado", "status" : "KO", "message" : "Error en el proceso" + str(e), "data" : "", "receiver" : sender}       
    
    
#Consultas a la BBDD

def getActual():

    cur = conn.cursor()

    cur.execute("""SELECT * from establecimiento where id_establecimiento=1""",)

    results = cur.fetchall()
    number = ""
    
    for row in results:
        number = row[3]
    
    return number

def getMax():
    
    cur = conn.cursor()

    cur.execute("""SELECT * from establecimiento where id_establecimiento=1""",)

    results = cur.fetchall()
    number = ""
    
    for row in results:
        number = row[4]
        
    return number
    
def existLogin(nick, pwd):

    cur = conn.cursor()

    cur.execute("""SELECT count(*) from empleado WHERE nickname = %s AND pwd = %s""", [str(nick),str(pwd)])
    results = cur.fetchone()
    for r in results:
        print(r)
        
   
    return r

def existCert(cert):
    cur = conn.cursor()

    cur.execute("""SELECT count(*) from certificados WHERE id_certificado = %s AND uso = 0""", [str(cert)])
    results = cur.fetchone()
    for r in results:
        print(r)
        
    return r

def existNick(nick):
  
    nick_s = str(nick)
    cur = conn.cursor()
    cur.execute("""SELECT count(*) from empleado WHERE nickname = %s """, [nick_s])
   
    results = cur.fetchone()

    for r in results:
        print(r)
   
    return r
    
    
#--------fin consultas BBDD---------------


#Dar de alta a los clientes MQTT en los diferentes threads

  
def alta_conexion(topic_pub, topic_sub, thread):
    client_sub = connect_mqtt(thread + "sub", thread)
    client_pub = connect_mqtt2(thread + "pub", thread)
    subscribe(client_sub, client_pub, topic_pub, topic_sub)
    client_pub.loop_start()
    client_sub.loop_forever()
    

def initial_message():
    print('\n\n\n------------ Bienvenido al servidor -------------\n')
    print('\nSe cargarán las dependencias con los tópicos de los dispositivos empleado....')
    
def run():
    initial_message()
    
    threads = []
    
    thread_register = threading.Thread(target = alta_conexion, args = (topic_pub_reg, topic_sub_reg, "thread 1"))
    thread_login = threading.Thread(target = alta_conexion, args = (topic_pub_log, topic_sub_log, "thread 2"))

    
    threads.append(thread_register)
    threads.append(thread_login)

    
    thread_register.start()
    print("\n")
    time.sleep(5)
    thread_login.start()
    print("\n")
    time.sleep(5)

    
if __name__ == '__main__':
    run()




