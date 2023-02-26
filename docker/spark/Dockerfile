#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM apache/spark:v3.3.2
ENV SPARK_HOME=/opt/spark
WORKDIR /

# Reset to root to run installation tasks
USER 0

RUN mkdir ${SPARK_HOME}/python
RUN apt-get update && \
    apt install -y python3 python3-pip wget && \
    pip3 install --upgrade pip setuptools && \
    # Removed the .cache to save space
    rm -r /root/.cache && rm -rf /var/cache/apt/*

RUN rm -f ${SPARK_HOME}/jars/aws-java-sdk-bundle-*.jar
RUN wget --quiet https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P ${SPARK_HOME}/jars/
RUN wget --quiet https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P ${SPARK_HOME}/jars/
RUN wget --quiet https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar -P ${SPARK_HOME}/jars/
RUN wget --quiet https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar -P ${SPARK_HOME}/jars/
RUN chmod 0644 ${SPARK_HOME}/jars/aws-java-sdk-bundle*.jar
RUN chmod 0644 ${SPARK_HOME}/jars/hadoop-aws*.jar
RUN chmod 0644 ${SPARK_HOME}/jars/delta-core*.jar
RUN chmod 0644 ${SPARK_HOME}/jars/delta-storage*.jar

COPY python/pyspark ${SPARK_HOME}/python/pyspark
COPY python/lib ${SPARK_HOME}/python/lib

WORKDIR /opt/spark/work-dir

COPY requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir -r ./requirements.txt

ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
