FROM python:3.10

ENV POETRY_HOME="/etc/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -

WORKDIR /sql_data_service
COPY ./poetry.lock ./pyproject.toml ./
RUN poetry env use system
RUN poetry install --no-dev

COPY ./src ./src
RUN poetry install

EXPOSE 80

CMD ["sh", "-c", "poetry run uvicorn sql_data_service.app:app --host 0.0.0.0 --port 80"]
