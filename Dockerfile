FROM python:3.8-slim
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "./src/main.py", "log_config.conf"]
