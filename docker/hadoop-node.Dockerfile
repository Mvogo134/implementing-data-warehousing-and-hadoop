FROM ubuntu:22.04

# Non-interactive installs
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget curl ca-certificates python3 python3-pip net-tools sudo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ---------- Java ----------
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# ---------- Hadoop 3.3.6 ----------
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# ---------- Hive 4.0.0 ----------
ENV HIVE_VERSION=4.0.0
ENV HIVE_HOME=/opt/hive
RUN wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz -O /tmp/hive.tar.gz && \
    tar -xzf /tmp/hive.tar.gz -C /opt && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} && \
    rm /tmp/hive.tar.gz
ENV PATH=${HIVE_HOME}/bin:$PATH

# ---------- Pig 0.17.0 ----------
ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig
RUN wget -q https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz -O /tmp/pig.tar.gz && \
    tar -xzf /tmp/pig.tar.gz -C /opt && \
    mv /opt/pig-${PIG_VERSION} ${PIG_HOME} && \
    rm /tmp/pig.tar.gz
ENV PATH=${PIG_HOME}/bin:$PATH
ENV PIG_HOME=/opt/pig

# ---------- Hadoop single-node config ----------
WORKDIR ${HADOOP_HOME}/etc/hadoop

# Core config: FS default name = hdfs://localhost:9000
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property>\n\
    <name>fs.defaultFS</name>\n\
    <value>hdfs://localhost:9000</value>\n\
  </property>\n\
</configuration>' > core-site.xml

# HDFS config: replication=1, data dir
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property>\n\
    <name>dfs.replication</name>\n\
    <value>1</value>\n\
  </property>\n\
  <property>\n\
    <name>dfs.permissions.enabled</name>\n\
    <value>false</value>\n\
  </property>\n\
</configuration>' > hdfs-site.xml

# MapReduce config
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property>\n\
    <name>mapreduce.framework.name</name>\n\
    <value>local</value>\n\
  </property>\n\
</configuration>' > mapred-site.xml

# ENV for Hadoop binaries
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV YARN_HOME=${HADOOP_HOME}

# Allow ssh-free namenode start
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

# ---------- Hive Metastore (embedded Derby for single-node) ----------
RUN mkdir -p /opt/hive/conf && \
    cp ${HIVE_HOME}/conf/hive-default.xml.template /opt/hive/conf/hive-site.xml 2>/dev/null || true
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property>\n\
    <name>javax.jdo.option.ConnectionURL</name>\n\
    <value>jdbc:derby:;databaseName=/tmp/metastore_db;create=true</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.metastore.warehouse.dir</name>\n\
    <value>/user/hive/warehouse</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.exec.local.scratchdir</name>\n\
    <value>/tmp/hive_scratch</value>\n\
  </property>\n\
  <property>\n\
    <name>hive.exec.scratchdir</name>\n\
    <value>/tmp/hive_scratch</value>\n\
  </property>\n\
</configuration>' > /opt/hive/conf/hive-site.xml

# ---------- Entry script ----------
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace
EXPOSE 9870 8088 10000 10002 9000

ENTRYPOINT ["/entrypoint.sh"]