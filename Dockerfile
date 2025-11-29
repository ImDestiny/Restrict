FROM python:3.10.8-slim-buster
WORKDIR /app

# Forces Python to print logs immediately
ENV PYTHONUNBUFFERED=1

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "restrict_bot.py"]
