import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import logging

class SankhyaAPIDataCollector:
    """
    Coletor de dados adaptado para consumir a API do Sankhya.
    Utiliza a API Gateway oficial do Sankhya para extrair dados de logs de eventos.
    """
    
    def __init__(self, base_url, app_key, sankhya_id, password, token=None):
        """
        Inicializa o coletor com as credenciais da API Sankhya.
        
        Args:
            base_url (str): URL base da API Gateway Sankhya
            app_key (str): Chave de aplicação (AppKey)
            sankhya_id (str): Email do SankhyaID
            password (str): Senha do SankhyaID
            token (str, optional): Token de acesso (se já disponível)
        """
        self.base_url = base_url.rstrip('/')
        self.app_key = app_key
        self.sankhya_id = sankhya_id
        self.password = password
        self.token = token
        self.session_id = None
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def authenticate(self):
        """
        Realiza a autenticação na API Gateway do Sankhya.
        
        Returns:
            bool: True se a autenticação foi bem-sucedida
        """
        try:
            auth_url = f"{self.base_url}/mge/service.sbr?serviceName=MobileLoginSP.login"
            
            auth_payload = {
                "serviceName": "MobileLoginSP.login",
                "requestBody": {
                    "NOMUSU": self.sankhya_id,
                    "INTERNO": self.password,
                    "KEEPCONNECTED": "S"
                }
            }
            
            headers = {
                "Content-Type": "application/json",
                "AppKey": self.app_key,
                "Authorization": f"Bearer {self.token}" if self.token else ""
            }
            
            response = requests.post(auth_url, json=auth_payload, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "1":
                    self.session_id = result.get("responseBody", {}).get("jsessionid")
                    self.logger.info("Autenticação realizada com sucesso")
                    return True
                else:
                    self.logger.error(f"Erro na autenticação: {result.get('statusMessage')}")
                    return False
            else:
                self.logger.error(f"Erro HTTP na autenticação: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Exceção durante autenticação: {str(e)}")
            return False
    
    def execute_query(self, sql_query, max_results=1000):
        """
        Executa uma consulta SQL através da API do Sankhya.
        
        Args:
            sql_query (str): Query SQL a ser executada
            max_results (int): Número máximo de resultados
            
        Returns:
            dict: Resultado da consulta
        """
        try:
            query_url = f"{self.base_url}/mge/service.sbr?serviceName=DbExplorerSP.executeQuery"
            
            query_payload = {
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql": sql_query,
                    "limit": max_results
                }
            }
            
            headers = {
                "Content-Type": "application/json",
                "AppKey": self.app_key,
                "Authorization": f"Bearer {self.token}" if self.token else "",
                "Cookie": f"JSESSIONID={self.session_id}" if self.session_id else ""
            }
            
            response = requests.post(query_url, json=query_payload, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "1":
                    return result.get("responseBody", {})
                else:
                    self.logger.error(f"Erro na consulta: {result.get('statusMessage')}")
                    return None
            else:
                self.logger.error(f"Erro HTTP na consulta: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"Exceção durante consulta: {str(e)}")
            return None
    
    def collect_process_events(self, days_back=30):
        """
        Coleta eventos de processos usando a view criada no banco Sankhya.
        
        Args:
            days_back (int): Número de dias para buscar dados históricos
            
        Returns:
            pandas.DataFrame: DataFrame com os eventos coletados
        """
        try:
            # Query para buscar dados da view de process mining
            sql_query = f"""
            SELECT 
                case_id,
                activity,
                timestamp,
                company_code,
                partner_code,
                seller_code,
                order_value,
                operation_type,
                resource,
                estimated_duration_minutes,
                process_category
            FROM VW_PROCESS_MINING_EVENTS
            WHERE timestamp >= SYSDATE - {days_back}
            ORDER BY case_id, timestamp
            """
            
            self.logger.info(f"Coletando eventos dos últimos {days_back} dias...")
            
            result = self.execute_query(sql_query)
            
            if result and "rows" in result:
                # Converter resultado para DataFrame
                columns = [field["name"] for field in result.get("fields", [])]
                rows = result["rows"]
                
                df = pd.DataFrame(rows, columns=columns)
                
                # Converter timestamp para datetime
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                self.logger.info(f"Coletados {len(df)} eventos de {df['case_id'].nunique()} casos únicos")
                
                return df
            else:
                self.logger.warning("Nenhum dado retornado da consulta")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Erro ao coletar eventos: {str(e)}")
            return pd.DataFrame()
    
    def preprocess_data(self, df):
        """
        Pré-processa os dados coletados para análise de Process Mining.
        
        Args:
            df (pandas.DataFrame): DataFrame com dados brutos
            
        Returns:
            pandas.DataFrame: DataFrame pré-processado
        """
        try:
            if df.empty:
                self.logger.warning("DataFrame vazio para pré-processamento")
                return df
            
            self.logger.info("Iniciando pré-processamento dos dados...")
            
            # Renomear colunas para padrão PM4Py
            df_processed = df.rename(columns={
                'case_id': 'case:concept:name',
                'activity': 'concept:name',
                'timestamp': 'time:timestamp',
                'resource': 'org:resource'
            })
            
            # Garantir que case_id e activity sejam strings
            df_processed['case:concept:name'] = df_processed['case:concept:name'].astype(str)
            df_processed['concept:name'] = df_processed['concept:name'].astype(str)
            
            # Ordenar por caso e timestamp
            df_processed = df_processed.sort_values(['case:concept:name', 'time:timestamp'])
            
            # Adicionar informações de duração entre atividades
            df_processed['duration_seconds'] = df_processed.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds().fillna(0)
            
            # Adicionar informações de sequência
            df_processed['activity_sequence'] = df_processed.groupby('case:concept:name').cumcount() + 1
            
            self.logger.info(f"Pré-processamento concluído. {len(df_processed)} eventos processados")
            
            return df_processed
            
        except Exception as e:
            self.logger.error(f"Erro no pré-processamento: {str(e)}")
            return df
    
    def save_to_csv(self, df, filename="sankhya_process_events.csv"):
        """
        Salva o DataFrame em arquivo CSV.
        
        Args:
            df (pandas.DataFrame): DataFrame a ser salvo
            filename (str): Nome do arquivo
        """
        try:
            df.to_csv(filename, index=False)
            self.logger.info(f"Dados salvos em {filename}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar arquivo: {str(e)}")
    
    def get_process_summary(self, df):
        """
        Gera um resumo dos processos coletados.
        
        Args:
            df (pandas.DataFrame): DataFrame com dados processados
            
        Returns:
            dict: Resumo dos dados
        """
        try:
            if df.empty:
                return {"error": "DataFrame vazio"}
            
            summary = {
                "total_events": len(df),
                "unique_cases": df['case:concept:name'].nunique() if 'case:concept:name' in df.columns else 0,
                "unique_activities": df['concept:name'].nunique() if 'concept:name' in df.columns else 0,
                "date_range": {
                    "start": df['time:timestamp'].min().isoformat() if 'time:timestamp' in df.columns else None,
                    "end": df['time:timestamp'].max().isoformat() if 'time:timestamp' in df.columns else None
                },
                "activity_frequency": df['concept:name'].value_counts().to_dict() if 'concept:name' in df.columns else {},
                "process_categories": df['process_category'].value_counts().to_dict() if 'process_category' in df.columns else {}
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar resumo: {str(e)}")
            return {"error": str(e)}

# Exemplo de uso
if __name__ == "__main__":
    # Configurações da API (devem ser fornecidas pelo usuário)
    config = {
        "base_url": "https://api.sankhya.com.br",  # URL base da API Gateway
        "app_key": "SUA_APP_KEY_AQUI",  # AppKey fornecida pela Sankhya
        "sankhya_id": "seu.email@empresa.com",  # Email do SankhyaID
        "password": "sua_senha_aqui",  # Senha do SankhyaID
        "token": "SEU_TOKEN_AQUI"  # Token de acesso (se disponível)
    }
    
    # Criar instância do coletor
    collector = SankhyaAPIDataCollector(**config)
    
    # Autenticar
    if collector.authenticate():
        # Coletar dados dos últimos 30 dias
        raw_data = collector.collect_process_events(days_back=30)
        
        if not raw_data.empty:
            # Pré-processar dados
            processed_data = collector.preprocess_data(raw_data)
            
            # Salvar em CSV
            collector.save_to_csv(processed_data, "sankhya_process_events.csv")
            
            # Gerar resumo
            summary = collector.get_process_summary(processed_data)
            print("Resumo dos dados coletados:")
            print(json.dumps(summary, indent=2, default=str))
        else:
            print("Nenhum dado foi coletado")
    else:
        print("Falha na autenticação")

