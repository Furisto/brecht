FROM gitpod/workspace-full

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v3.19.1/protoc-3.19.1-linux-x86_64.zip &&\
    sudo unzip protoc-3.19.1-linux-x86_64.zip -d /usr/local/protobuf &&\
    sudo chown -R gitpod /usr/local/protobuf &&\
    rm protoc-3.19.1-linux-x86_64.zip

ENV PATH="/usr/local/protobuf:${PATH}"
