# For more information, please refer to https://aka.ms/vscode-docker-python
#
FROM python:3.9

EXPOSE 8000

#
WORKDIR /code

#
COPY ./requirements.txt /code/requirements.txt

#
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

#
COPY . /code

#
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000","--reload"]
