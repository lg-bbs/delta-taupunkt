import RPi.GPIO as GPIO


class Fan:
    def __init__(self):
        self.relay_pin = 21 
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.relay_pin, GPIO.OUT)
        # Servo
        self.servo_pin = 25 #TODO BCM 25 (BOARD 22), BCM 26 (BOARD 37)
        GPIO.setup(self.servo_pin, GPIO.OUT)
        self.servo_pwm = GPIO.PWM(self.servo_pin, 50)  # 50 Hz

    def turn_on(self):
        GPIO.output(self.relay_pin, GPIO.LOW)
        #Servo
        self.servo_pwm.start(0)
        self.servo_pwm.ChangeDutyCycle(7.5) #TODO speed

    def turn_off(self):
        GPIO.output(self.relay_pin, GPIO.HIGH)
        #Servo
        self.servo_pwm.stop()