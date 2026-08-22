FROM itversity/itvdelab

USER root

# 1. Install Python and sshpass
RUN apt-get update && \
    apt-get install -y python3 python3-pip sshpass && \
    rm -rf /var/lib/apt/lists/*

# 2. Create python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# 3. Install Python dependencies
RUN pip3 install --no-cache-dir \
    pyspark \
    python-dotenv \
    PyYAML

# 4. Setup environment variables
RUN echo 'export PYSPARK_PYTHON=python' >> /home/itversity/.bashrc && \
    echo 'export SPARK_HOME=/opt/spark-2.4.8-bin-hadoop2.7' >> /home/itversity/.bashrc && \
    echo 'export PATH=$SPARK_HOME/bin:$PATH' >> /home/itversity/.bashrc

# 5. Create Spark jobs directory
RUN mkdir -p /spark_jobs && \
    chown itversity:itversity /spark_jobs

# Copy Hive Conf File
#COPY hive/hive-site.xml /opt/custom-hive-site.xml

#Hive Start up script
#COPY utilities/start-hive.sh /usr/local/bin/start-hive.sh

RUN chmod +x /usr/local/bin/start-hive.sh

# Switch back to itversity user
USER itversity

WORKDIR /spark_jobs

# ENTRYPOINT ["/usr/local/bin/start-hive.sh"]