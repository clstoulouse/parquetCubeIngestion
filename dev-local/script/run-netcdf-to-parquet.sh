#!/usr/bin/env bash

##########
##Script parameters, edit if needed
##########

APP_HOME=$(cd "$(readlink -f "../../daemon/parquet-cube-crawler")" && pwd)
LIB_DIR="${APP_HOME}/target"
FULLY_QUALIFIED_CLASSNAME="fr.cls.bigdata.metoc.ingestion.job.StartCrawling"
PROJECT_VERSION="1.0.0"


##########
##########


show_usage() {
    echo "USAGE: ${0} [OPTION] [ARGS]"
    echo "DESCRIPTION: Runs a toolkit script"
    echo "OPTIONS:"
    echo "  -h|--help:      Prints the command usage and then exits"
    echo "ARGUMENTS:"
    echo "  [ARGS]:         Arguments of the job and/or jvm options as Java Args."
}

# Controlling args step 1
if [ $# -lt 1 ]; then
	echo "Need at least conf file as argument (Exemple : -Dconfig.file=J:/dev/parquetCubeIngestion/dev-local/application.conf)"
	show_usage
	exit 2
fi

# Managing args
JAVA_ARGS=""
PROG_ARGS=""

for i in $@
do
	case $i in
		-D*)JAVA_ARGS="$JAVA_ARGS $i";;
		-X*)JAVA_ARGS="$JAVA_ARGS $i";;
	esac
done

# Controlling args step 2
while [ $# -gt 0 ]; do
    case $1 in
        -D*)
          shift
          ;;
		    -X*)
          shift
          ;;
        *)
          echo "Unknown parameter ${1}"
          show_usage
          exit 0
          ;;
	esac
done

# Executing class
MAIN_JAR="${LIB_DIR}/parquet-cube-crawler-${PROJECT_VERSION}.jar"
CLASSPATH="${MAIN_JAR}:${APP_HOME}"
for f in $(find "${LIB_DIR}" -name '*.jar') ; do
    if [ "${f}" != "${MAIN_JAR}" ]; then
           CLASSPATH="${f}:${CLASSPATH}"
    fi
done

exec java \
    -cp "${CLASSPATH}" \
    ${JAVA_ARGS} \
    "${FULLY_QUALIFIED_CLASSNAME}"