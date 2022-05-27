import socket
import os
import liquidcrystal_i2c
import time

lcd = liquidcrystal_i2c.LiquidCrystal_I2C(0x27, 1, numlines=4)
ip_str = "IP: ..." 
lcd.printline(0,"**FaceAccess brain**")
lcd.printline(2,"Loading....")
lcd.printline(3,ip_str)

i = 0

def getIp():
    global i

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        IP = s.getsockname()[0]
        ip_ok = "IP: " + IP + "   " 
        port = 1883
        s.close()
    
        time.sleep(2)
        lcd.printline(3,ip_ok)
        os.system('python3 /home/pi/Desktop/SERVER/capacity_access_server.py -h ' +IP+' -p '+str(port) + ' &')

    except OSError:
        i = i + 1
        error = "Not connected (" + str(i) + ")"
        lcd.printline(3,error)
        time.sleep(3)
        getIp()
        
getIp()
