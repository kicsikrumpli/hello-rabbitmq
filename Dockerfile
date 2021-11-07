FROM python:3.9

# setup directories
RUN mkdir -p /bunny/hello_rabbitmq
WORKDIR /bunny

# copy
COPY hello_rabbitmq hello_rabbitmq
COPY poetry.lock .
COPY pyproject.toml .
COPY entrypoint.sh .

# set up poetry
RUN pip install poetry
RUN poetry install

ENTRYPOINT [ "./entrypoint.sh" ]




