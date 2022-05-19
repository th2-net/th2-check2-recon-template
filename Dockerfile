FROM python:3.8
WORKDIR /usr/src/app
COPY . .
ARG PROTOBUF_DESCRIPTION
RUN pip install -r requirements.txt && \
    pip install th2-box-descriptor-generator==0.0.1.dev2346263217 && \
    python3 -m th2_box_descriptor_generator.generate_service_descriptions && \
    PROTOBUF_DESCRIPTION=$(cat service_proto_description.json) && \
    echo $PROTOBUF_DESCRIPTION
LABEL protobuf-description-base64=$PROTOBUF_DESCRIPTION
ENTRYPOINT [ "python", "./src/main.py"]
