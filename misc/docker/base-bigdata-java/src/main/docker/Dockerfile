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

###########

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

###########

# This is introduced as a workaround for : https://github.com/fabric8io/docker-maven-plugin/issues/894
ENV DOCKER_JAVA_HOME /docker-java-home

ENV JRE_DOWNLOAD_URL "http://mirror.centos.org/centos/7/updates/x86_64/Packages/java-1.8.0-openjdk-1.8.0.292.b10-1.el7_9.x86_64.rpm"
ENV JAVA_HOME ${DOCKER_JAVA_HOME}

USER root

RUN curl -v -s -L ${JRE_DOWNLOAD_URL} > /tmp/jre.rpm && \
    yum -y localinstall /tmp/jre.rpm && \
    rm /tmp/jre.rpm && \
    rm -rf /var/cache/yum && \
    find /tmp /var/tmp /run /var/log -mindepth 1 -delete -print && \
    ln -svT $(readlink -f /usr/bin/java | sed "s:/bin/java::") "${DOCKER_JAVA_HOME}"

USER bigdata

###########

ENV JAVA_APP_DIR=/opt/bigdata

ENV JAVA_LIB_DIR=${JAVA_APP_DIR}/lib \
    JAVA_BIN_DIR=${JAVA_APP_DIR}/bin \
    JAVA_CONF_DIR=${JAVA_APP_DIR}/conf \
    PRESTART_HOOKS_DIR=${JAVA_APP_DIR}/pre-start.d \
    DEV_DIR=${JAVA_APP_DIR}/dev

ENV CLASSPATH_FILE="${DEV_DIR}/classpath" \
    JAVA_INIT_MEM_RATIO=25 \
    JAVA_MAX_MEM_RATIO=70 \
    DATA_DIR=${JAVA_APP_DIR}/data

USER root

RUN mkdir -p ${JAVA_APP_DIR} ${JAVA_LIB_DIR} ${JAVA_BIN_DIR} ${JAVA_CONF_DIR} ${PRESTART_HOOKS_DIR} ${DEV_DIR} ${DATA_DIR} && \
    chown -R bigdata:bigdata ${JAVA_APP_DIR} && \
    chmod a+rwx ${DEV_DIR}

COPY --chown=bigdata:bigdata files-bigdata-java-app/ ${JAVA_APP_DIR}

RUN find "${JAVA_APP_DIR}" -type f -name '*.sh' -print -exec chmod +rx {} \;

USER bigdata

WORKDIR ${JAVA_APP_DIR}
CMD ["/opt/bigdata/bin/start-java-app.sh"]

##########

USER root

RUN ${docker.users.creation} && \
    chown -R ${docker.user.name}:${docker.group.name} ${JAVA_APP_DIR}

USER ${docker.user.name}
