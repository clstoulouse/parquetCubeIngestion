FROM centos:7

RUN yum update -y \
    && yum install -y \
      bind-utils \
      net-tools \
      vim \
      wget \
      htop \
      nano \
      dos2unix \
      telnet \
    && localedef -i en_US -f UTF-8 en_US.UTF-8 \
    && localedef -i fr_FR -f UTF-8 fr_FR.UTF-8 \
    && echo 'LANG="en_US.UTF-8"' > /etc/locale.conf \
    && groupadd -g 10070 ope \
    && yum clean all \
    && rm -rf /var/cache/yum


ENV LANG en_US.UTF-8

CMD ["/bin/bash"]

##########

ENV DOCKER_ENTRYPOINT_SCRIPTS="/docker-entry-point.d"

USER root

COPY files/docker-entry-point/ /

RUN groupadd --gid 1000 bigdata && \
    useradd --uid 1000 --gid 1000 bigdata && \
    chmod a+rw /etc/passwd /etc/group && \
    find "${DOCKER_ENTRYPOINT_SCRIPTS}" -name '*.sh' -exec chmod +rx {} \; && \
    chmod +rx /docker-entry-point.sh

USER bigdata

ENTRYPOINT ["/docker-entry-point.sh"]

##########

# This is introduced as a workaround for : https://github.com/fabric8io/docker-maven-plugin/issues/894
ENV DOCKER_JAVA_HOME /docker-java-home

ENV JRE_DOWNLOAD_URL "http://mirror.centos.org/centos/7/updates/x86_64/Packages/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64.rpm"
ENV JAVA_HOME ${DOCKER_JAVA_HOME}

USER root

RUN curl -v -s -L ${JRE_DOWNLOAD_URL} > /tmp/jre.rpm && \
    yum -y localinstall /tmp/jre.rpm && \
    rm /tmp/jre.rpm && \
    rm -rf /var/cache/yum && \
    find /tmp /var/tmp /run /var/log -mindepth 1 -delete -print && \
    ln -svT $(readlink -f /usr/bin/java | sed "s:/bin/java::") "${DOCKER_JAVA_HOME}"

ENV HADOOP_DOWNLOAD_URL="http://archive.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz" \
    DOCKER_HADOOP_HOME="/opt/hadoop"

ENV HADOOP_HOME=${DOCKER_HADOOP_HOME}

# WARNING If you change ${PATH:-} by ${PATH}, the docker maven plugin will mess up (substitution will happen too early)
ENV PATH="${PATH:-}:${DOCKER_HADOOP_HOME}/bin"

RUN mkdir -p "${DOCKER_HADOOP_HOME}" && \
    curl -v -s --fail "${HADOOP_DOWNLOAD_URL}" | tar -xzvf - --strip 1 -C "${DOCKER_HADOOP_HOME}" && \
    cp -v ${DOCKER_HADOOP_HOME}/share/hadoop/tools/lib/jackson-* ${DOCKER_HADOOP_HOME}/share/hadoop/common/lib/ && \
    chown root:root -R "${DOCKER_HADOOP_HOME}"

COPY hadoop-libs/* ${DOCKER_HADOOP_HOME}/share/hadoop/common/lib/
COPY bin/* ${DOCKER_HADOOP_HOME}/bin/

RUN chmod +x ${DOCKER_HADOOP_HOME}/bin/*

RUN ${docker.users.creation}

USER ${docker.user.name}