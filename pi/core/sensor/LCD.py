#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import board
import busio
from core.model.Measurement import Measurement
import adafruit_character_lcd.character_lcd_i2c as character_lcd
from pi.core.API import postErrorException

class LCD:
    def __init__(self):
        lcd_columns = 16
        lcd_rows = 2
        i2c = busio.I2C(board.SCL, board.SDA)
        self.lcd = character_lcd.Character_LCD_I2C(i2c, lcd_columns, lcd_rows, 0x21)

    def showData(self, m: Measurement):
        try:
            self.lcd.backlight = True
            self.lcd.message = f"{m.txt()}"
        except KeyboardInterrupt:
            self.lcd.clear()
            self.lcd.backlight = False
        except Exception as e:
            postErrorException(e, "LCD")