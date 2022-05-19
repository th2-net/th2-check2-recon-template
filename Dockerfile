FROM python:3.8
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt
ARG DESCRIPTION_JSON
LABEL protobuf-description-base64=$DESCRIPTION_JSON
ENTRYPOINT [ "python", "./src/main.py"]