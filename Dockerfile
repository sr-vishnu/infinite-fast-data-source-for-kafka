FROM python:3.9-slim-buster

ADD application /application
ADD config /config
ADD messages /messages
ADD requirements.txt /requirements.txt

RUN pip3 install -r requirements.txt

CMD [ "python3","/application/main.py"]