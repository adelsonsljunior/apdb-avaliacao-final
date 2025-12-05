#!/bin/bash

CONTAINER_NAME="postgres-avaliacao"
DB_NAME="olist"
DB_USER="postgres"
BACKUP_DIR="/data/backup/postgresql"
DATE_FORMAT=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/olist_full_${DATE_FORMAT}.dump"

mkdir -p "${BACKUP_DIR}"

echo "Iniciando backup do banco de dados '${DB_NAME}' no container '${CONTAINER_NAME}'..."
echo "Salvando em: ${BACKUP_FILE}"

docker exec -t "${CONTAINER_NAME}" pg_dump -U "${DB_USER}" -d "${DB_NAME}" -Fc > "${BACKUP_FILE}"

if [ $? -eq 0 ]; then
    FILE_SIZE=$(du -h "${BACKUP_FILE}" | awk '{print $1}')
    echo "Backup concluído com sucesso!"
    echo "Tamanho do arquivo: ${FILE_SIZE}"
    
else
    echo "ERRO: O backup falhou. Verifique se o container '${CONTAINER_NAME}' está em execução."
    exit 1
fi

exit 0
