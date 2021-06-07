set-namespace "metocdev"

PACKAGES_BASE_URL="http://todefine.helmrepo.host.fr/nexus/repository/packages-to-deploy/parquet-cube/helm"
PACKAGES_VERSION="1.0.0"

# Packages

package --name "parquet-cube-storage" \
        --chart "${PACKAGES_BASE_URL}/${PACKAGES_VERSION}/parquet-cube-storage-${PACKAGES_VERSION}.tgz"

package --name "parquet-cube-ingestion" \
        --chart "${PACKAGES_BASE_URL}/${PACKAGES_VERSION}/parquet-cube-ingestion-chart-${PACKAGES_VERSION}.tgz"

