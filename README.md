# ETL_Processes_Alex_Fomenko_DZ3
3 домашние задание по курсу ETL процессы на тему «Загрузка данных в целевую систему». Студент Фоменко Алексей.
Содержание .env (нужно создать после клонирования):
AIRFLOW_UID=1000
AIRFLOW_GID=0
# Структура проекта:
<pre>
etl_Processes_Alex_Fomenko_hw2/
├── dags/
│   ├──full_load_iot.py
│   ├──incremental_load_iot.py 
│   └──operate_IOT.py
├── config/
│   └── ..
├── logs/
│   └── ..
├── plugins/
│   └── ..
├── data/
│   └── ..
├── docker-compose.yaml
├── .env
└── README.md

Результат работы дага можно посмотреть через DBeaver(порт 5432, логин и пароль - postgres):
  Таблица public.iot_clean - исходная таблица
  Таблица loaded_data.full_load - полная прогрузка исторических данных
  Таблица loaded_data.incremental_load - инкрементальная загрузка (за последние несколько дней)
</pre>

#Запуск сервиса: <br/>
echo -e "AIRFLOW_UID=1000\nAIRFLOW_GID=0" > .env<br/>
docker compose up -d <br/>
Адрес http://localhost:8080/ <br/>
Логин и пароль - airflow <br/>
