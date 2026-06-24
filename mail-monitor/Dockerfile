FROM python:3.11-alpine

WORKDIR /app

RUN pip install docker requests

# Copia el monitor.py que está en su misma carpeta hacia el contenedor
COPY monitor.py .

CMD ["python", "-u", "monitor.py"]