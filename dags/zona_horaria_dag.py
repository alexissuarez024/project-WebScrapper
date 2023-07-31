"""
This script simply returns the current date that is used in the different scripts
"""

from datetime import timedelta, datetime
from pytz import timezone
import pytz

fecha_y_hora_actuales = datetime.now()
zona_horaria = timezone('America/Rosario')
fecha_y_hora_rosario = fecha_y_hora_actuales.astimezone(zona_horaria)
rosario_date = fecha_y_hora_rosario.strftime('%d-%m-%Y')
date_now = fecha_y_hora_rosario.strftime("%d-%m-%Y %H:%M:%S")
