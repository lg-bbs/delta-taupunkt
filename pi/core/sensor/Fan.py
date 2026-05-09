#import RPi.GPIO as GPIO
#
#
#class Fan:
#    def __init__(self):
#        self.relay_pin = 21 
#        GPIO.setmode(GPIO.BCM)
#        GPIO.setup(self.relay_pin, GPIO.OUT)
#
#    def turn_on(self):
#        GPIO.output(self.relay_pin, GPIO.LOW)
#
#    def turn_off(self):
#        GPIO.output(self.relay_pin, GPIO.HIGH)