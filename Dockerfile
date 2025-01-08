FROM python:3.10

COPY ./src /src
COPY requirements.txt requirements.txt
COPY deploy_model.py deploy_model.py

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "deploy_model:app", "--host", "0.0.0.0", "--port", "8000"]