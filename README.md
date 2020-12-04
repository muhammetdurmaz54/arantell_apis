
#### Install requirements:

`pip install -r requirements.txt`

#### Run server:

`uvicorn api:app --reload`

#### API Documentation 

http://127.0.0.1:8000/docs

HTML documentation can be generated using src/scripts/export_docs.py
Offline Doc stored in /docs

#### Endpoints

Endpoints present in api.py file.

#### API Keys
API_KEY= Present in .env file.
API_KEY_NAME = `X-API-KEY`


#### Check status:

http://127.0.0.1:8000/api/status

Using curl:

`curl --request GET http://127.0.0.1:8000/api/status`



