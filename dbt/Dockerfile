FROM python:3.11.16

WORKDIR /usr/app/dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DBT_PROFILES_DIR=/usr/app/dbt
ENTRYPOINT ["dbt"]
