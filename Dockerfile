FROM python:3.8
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt && \
    pip install th2-box-descriptor-generator==0.0.1.dev2346263217 && \
    python3 -m th2_box_descriptor_generator.generate_service_descriptions
LABEL protobuf-description-base64=$(cat service_proto_description.json)
ENTRYPOINT [ "python", "./src/main.py"]
