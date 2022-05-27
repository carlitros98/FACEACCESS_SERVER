import os
import sys
import time


host = ""
port = 0

def error_message():
    print("\nError: argumentos incorrectos, debe seguir el siguiente formato: \n")
    print("$ python3 capacity_access_server -h <broker host> -p <broker port>")
    exit(1)

'''
Comprobamos el número de argumentos y los mismos antes de ejecutar los ficheros bootstrap

'''


if len(sys.argv) != 5:
    error_message()
    
else:
    host_ok = 0
    port_ok = 0
     
    fun = sys.argv[1]
     
    if fun == "-h":
        host = str(sys.argv[2])
        host_ok = 1
        
    elif fun == "-p":
        port = int(sys.argv[2])
        port_ok = 1
        if isinstance(port, int) == False:
            error_message()
     
    else:
        error_message() 


    fun2 = sys.argv[3]

    if fun2 == "-h":
        if host_ok == 0:
            host = str(sys.argv[4])
            host_ok = 1       
        else:
            error_message() 

        
    elif fun2 == "-p":
        if port_ok == 0:
            port = int(sys.argv[4])
            port_ok = 1 
                      
            if isinstance(port, int) == False:
                error_message()      
        else:
            error_message() 
     
     
    else:
        error_message()     

#INICIAMOS EL SERVIDOR

print("\nConfiguración: \n\nHOST: " + host + "\nPORT: " + str(port) + "\n")
os.system('sudo systemctl restart mosquitto')

time.sleep(2)
os.system('python3 /home/pi/Desktop/SERVER/servers/bootstrap\ empleado.py -h ' +host+' -p '+str(port) + ' &')
os.system('python3 /home/pi/Desktop/SERVER/servers/bootstrap\ dispositivo.py -h ' +host+' -p '+str(port) + ' &')


