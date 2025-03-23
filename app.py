
import requests
import os
import pandas as pd
import logging
from flask import Flask, jsonify
from dotenv import load_dotenv
from google.oauth2 import service_account
import base64
import json

load_dotenv()

credentials_json = base64.b64decode(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')).decode('utf-8')
credentials_info = json.loads(credentials_json)

CREDENCIALS = service_account.Credentials.from_service_account_info(credentials_info)

app = Flask(__name__)

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class GetApi:
  
  def __init__(self):
    self.TOKEN = os.getenv('TOKEN')
    self.url = 'https://api.aviationstack.com/v1/flights'
    self.CREDENTIALS = CREDENCIALS
    self.PROJECT_ID = os.getenv('PROJECT_ID')
    self.DESTINATION_TABLE = os.getenv('DESTINATION_TABLE')

  def _requestApi(self):
    params = {  
      'access_key': self.TOKEN,
      'offset': 0,
      'status': 'landed',
      'dep_iata': 'VCP'
    }
    
    try:
      response = requests.get(self.url, params=params)
      response.raise_for_status()
      return response
    except requests.exceptions.RequestException as e:
      logging.error(f'Error: {e} na função "_requestApi" linha 19')
      raise

  def __returnFetchApi(self) -> list:
    
    response = self._requestApi()
    
    if response.status_code != 200:
      logging.error(f'Erro: API retornou {response.status_code}')
      raise ValueError(f'Falha na requisição: {response.status_code}')
      
    try:
      response_json = response.json()
      return response_json['data']
  
    except (ValueError, KeyError) as e:
      logging.error(f'Erro na conversão para JSON na função: {e}')
      raise

    
  def _createPandasDf(self) -> pd.DataFrame:
   
    data = self.__returnFetchApi()
    
    if data is None or not data:
        logging.error('Erro: "__returnFetchApi" retornou None ou lista vazia (linha 35)')
        raise ValueError('Erro: "__returnFetchApi" retornou None ou lista vazia (linha 35)')
    try:
      df = pd.json_normalize(data)
      colunas_esperadas = ['flight_date', 'departure.airport', 'departure.scheduled', 'departure.actual',
                          'arrival.airport', 'arrival.iata', 'arrival.icao', 'arrival.scheduled',
                          'arrival.actual', 'airline.name', 'aircraft.registration', 'aircraft.iata',
                          'aircraft.icao', 'aircraft.icao24']
  
      df = df[[col for col in colunas_esperadas if col in df.columns]]
  
      if df.empty:
        logging.warning('Erro: Colunas esperadas não estão presentes no df ou o df está vazio.')
        return None
      return df
  
    except KeyError as e:
        logging.error(f'Erro no tratamento do Dataframe na função "__createPandasDf" linha 48: {e}')
        raise

  def _renameColumns(self) -> pd.DataFrame:
      
    df = self._createPandasDf()

    if df is None or df.empty:
        logging.error('Erro: "df = self._createPandasDf()" retornou vazio ou None')
        raise ValueError('Erro: "df = self._createPandasDf()" retornou vazio ou None')
    
    new_columns_names = {
      col: f"{col.split('.')[0]}{col.split('.')[1].title()}"
      for col in df.columns if '.' in col
    }
    
    df = df.rename(columns=new_columns_names)
    
    return df
  
  def insertDb(self):
    logging.info('Iniciando a função "insertDb"')

    try:
      insert_dataframe_bgq = self._renameColumns()
    except (KeyError, ValueError) as e:
      logging.warning(f'Retorno do dataframe da function: "_createPandasDf". Erro {e}')
      raise
    
    try:
      insert_dataframe_bgq.to_gbq(destination_table=self.DESTINATION_TABLE,
                  project_id=self.PROJECT_ID,
                  credentials=self.CREDENTIALS,
                  if_exists='append')
      logging.info("Dados inseridos no BigQuery com sucesso")
    except Exception as e:
      logging.warning(f'Erro ao inserir dados no Bigquery devido ao seguinte erro {e}')
      raise

@app.route('/')
def run_task():
    logging.info("Acessando a rota /")
    try:
        get_api = GetApi()
        
        logging.info("Iniciando insertDb")
        get_api.insertDb()
        
        logging.info("Dados inseridos com sucesso")
        
        return jsonify({"status": "success", "message": "Data inserted successfully"}), 200
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)













