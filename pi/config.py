API_ENDPOINT = "http://localhost:5001/api/"

INTERVAL = 3 # Intervall, welches zwischen Messungen abgewartet wird

TARGET_DELTA = 5.0 # Delta-TP ab dem Lüfter an geht
HYSTERESE = 1 # Puffer (nicht am Delta TP-Rand an und aus)
MIN_TEMP_INSIDE = 10 # Minimale Innentemperatur (darunter: Lüfter aus)
MIN_TEMP_OUTSIDE = -10 # Minimale Außentemperatur (darunter: Lüfter aus)