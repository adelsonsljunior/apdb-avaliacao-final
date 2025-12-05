# apdb-avaliacao-final

## Criação do ambiente

Suba o banco de dados
~~~sh
docker compose up -d
~~~

Crie as tabelas usando o script sql de criação
~~~sh
docker exec -i postgres-avaliacao psql -U postgres -d olist < sql/01_create_model.sql
~~~

## Carga dos Dados

Instale as dependências e execute o script em python

| Usando uv 

~~~sh
uv sync
~~~
~~~sh
uv run load_datasets.py
~~~

| Usando pip
~~~sh
pip install -r requirements.txt
~~~
~~~sh
python3 load_datasets.py
~~~

## Restrições de Integridade

Crie as restrições usando o script sql
~~~sh
docker exec -i postgres-avaliacao psql -U postgres -d olist < sql/03_integrity_constraints.sql
~~~

## Criar um usuário para o pessoal de BI

Crie o usuário para BI usando o script sql
~~~sh
docker exec -i postgres-avaliacao psql -U postgres -d olist < sql/04_create_bi_user.sql
~~~

## Estratégia de Backup

| Execute o script de backup
~~~sh
sudo ./backup.sh
~~~

| Definir cronjob
~~~sh
crontab -e
0 1 * * * ~/apdb-avaliacao-final/backup.sh
~~~