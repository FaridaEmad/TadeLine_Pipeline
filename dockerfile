FROM itversity/itvdelab

USER root

RUN apt-get update && \
    apt-get install -y sshpass && \
    rm -rf /var/lib/apt/lists/*

USER itversity