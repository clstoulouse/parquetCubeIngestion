FROM ${docker.base}

COPY --chown=${docker.user.name}:${docker.group.name} ${docker.supporting-files.dir}/ ${docker.working.dir}

ENV JAVA_MAIN_JAR_NAME "${project.build.finalName}.${project.packaging}"

ENV JAVA_MAIN_CLASS "fr.cls.bigdata.metoc.ingestion.job.StartCrawling"
