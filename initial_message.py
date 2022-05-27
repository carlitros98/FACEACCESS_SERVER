import socket
import os
import liquidcrystal_i2c
import time

lcd = liquidcrystal_i2c.LiquidCrystal_I2C(0x27, 1, numlines=4)
time.sleep(2)
lcd.clear()

lcd.printline(0,"**FaceAccess brain**")
lcd.printline(2,"Loading....")
time.sleep(1)
lcd.clearline(0)

