from flask import Flask, render_template, jsonify, request
import os
import json
from sankhya_api_data_collector import SankhyaAPIDataCollector
from process_analyzer import ProcessAnalyzer
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Configurações da API Sankhya (devem ser configuradas via variáveis de ambiente em produção)
SANKHYA_CONFIG = {
    "base_url": os.getenv("SANKHYA_API_URL", "https://api.sankhya.com.br"),
    "app_key": os.getenv("SANKHYA_APP_KEY", ""),
    "sankhya_id": os.getenv("SANKHYA_ID", ""),
    "password": os.getenv("SANKHYA_PASSWORD", ""),
    "token": os.getenv("SANKHYA_TOKEN", "")
}

@app.route('/')
def index():
    """Página principal da aplicação."""
    return render_template('index_sankhya.html')

@app.route('/api/test-connection', methods=['POST'])
def test_sankhya_connection():
    """Endpoint para testar a conexão com a API do Sankhya."""
    try:
        # Obter configurações do request (para permitir configuração dinâmica)
        config = request.json if request.json else SANKHYA_CONFIG
        
        # Criar instância do coletor
        collector = SankhyaAPIDataCollector(
            base_url=config.get("base_url", SANKHYA_CONFIG["base_url"]),
            app_key=config.get("app_key", SANKHYA_CONFIG["app_key"]),
            sankhya_id=config.get("sankhya_id", SANKHYA_CONFIG["sankhya_id"]),
            password=config.get("password", SANKHYA_CONFIG["password"]),
            token=config.get("token", SANKHYA_CONFIG["token"])
        )
        
        # Tentar autenticar
        if collector.authenticate():
            return jsonify({
                'status': 'success',
                'message': 'Conexão com API Sankhya estabelecida com sucesso',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Falha na autenticação com a API Sankhya'
            }), 401
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Erro ao conectar com API Sankhya: {str(e)}'
        }), 500

@app.route('/api/analyze-sankhya', methods=['POST'])
def analyze_sankhya_processes():
    """Endpoint para analisar processos usando dados reais do Sankhya."""
    try:
        # Obter parâmetros da requisição
        request_data = request.json if request.json else {}
        days_back = request_data.get('days_back', 30)
        config = request_data.get('config', SANKHYA_CONFIG)
        
        # Criar instância do coletor
        collector = SankhyaAPIDataCollector(
            base_url=config.get("base_url", SANKHYA_CONFIG["base_url"]),
            app_key=config.get("app_key", SANKHYA_CONFIG["app_key"]),
            sankhya_id=config.get("sankhya_id", SANKHYA_CONFIG["sankhya_id"]),
            password=config.get("password", SANKHYA_CONFIG["password"]),
            token=config.get("token", SANKHYA_CONFIG["token"])
        )
        
        # Autenticar
        if not collector.authenticate():
            return jsonify({
                'status': 'error',
                'message': 'Falha na autenticação com a API Sankhya'
            }), 401
        
        # Coletar dados
        raw_data = collector.collect_process_events(days_back=days_back)
        
        if raw_data.empty:
            return jsonify({
                'status': 'warning',
                'message': 'Nenhum dado encontrado no período especificado',
                'suggestions': [
                    'Verifique se a view VW_PROCESS_MINING_EVENTS foi criada no banco de dados',
                    'Confirme se existem dados no período selecionado',
                    'Verifique as permissões de acesso às tabelas do Sankhya'
                ]
            })
        
        # Pré-processar dados
        processed_data = collector.preprocess_data(raw_data)
        
        # Salvar dados temporários
        temp_filename = f'sankhya_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        collector.save_to_csv(processed_data, temp_filename)
        
        # Analisar processos
        analyzer = ProcessAnalyzer(log_path=temp_filename)
        event_log = analyzer.load_and_convert_log()
        net, initial_marking, final_marking = analyzer.discover_process_model()
        
        # Gerar visualização
        process_image_path = f"static/sankhya_process_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        analyzer.visualize_process_model(net, initial_marking, final_marking, process_image_path)
        
        # Análise de conformidade
        aligned_traces = analyzer.analyze_conformance(net, initial_marking, final_marking)
        
        # Identificar gargalos
        analyzer.identify_bottlenecks(net, initial_marking, final_marking)
        
        # Gerar resumo dos dados
        data_summary = collector.get_process_summary(processed_data)
        
        # Gerar sugestões baseadas nos dados reais
        suggestions = generate_sankhya_suggestions(processed_data, aligned_traces)
        
        # Preparar resposta
        response = {
            'status': 'success',
            'message': 'Análise de processos Sankhya concluída com sucesso',
            'data_summary': data_summary,
            'process_model_image': '/' + process_image_path,
            'num_traces': len(aligned_traces),
            'fitness_perfect': sum(1 for trace in aligned_traces if trace["fitness"] == 1.0),
            'fitness_average': sum(trace["fitness"] for trace in aligned_traces) / len(aligned_traces) if aligned_traces else 0,
            'suggestions': suggestions,
            'analysis_timestamp': datetime.now().isoformat(),
            'data_period': {
                'days_analyzed': days_back,
                'start_date': data_summary.get('date_range', {}).get('start'),
                'end_date': data_summary.get('date_range', {}).get('end')
            }
        }
        
        # Limpar arquivo temporário
        try:
            os.remove(temp_filename)
        except:
            pass
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Erro na análise: {str(e)}',
            'suggestions': [
                'Verifique a conectividade com o banco de dados Sankhya',
                'Confirme se a view VW_PROCESS_MINING_EVENTS existe e está acessível',
                'Verifique as credenciais da API'
            ]
        }), 500

def generate_sankhya_suggestions(df, aligned_traces):
    """
    Gera sugestões específicas baseadas nos dados reais do Sankhya.
    
    Args:
        df (pandas.DataFrame): Dados processados
        aligned_traces (list): Traces alinhados da análise de conformidade
        
    Returns:
        list: Lista de sugestões
    """
    suggestions = []
    
    try:
        if df.empty:
            return ["Não foi possível gerar sugestões devido à falta de dados"]
        
        # Análise de atividades mais frequentes
        if 'concept:name' in df.columns:
            activity_counts = df['concept:name'].value_counts()
            most_frequent = activity_counts.index[0] if len(activity_counts) > 0 else None
            
            if most_frequent:
                suggestions.append(f"A atividade '{most_frequent}' é a mais frequente ({activity_counts[most_frequent]} ocorrências). Considere automatizar ou otimizar este processo.")
        
        # Análise de duração
        if 'duration_seconds' in df.columns:
            avg_duration = df['duration_seconds'].mean()
            if avg_duration > 3600:  # Mais de 1 hora
                suggestions.append(f"Duração média entre atividades é de {avg_duration/3600:.1f} horas. Identifique gargalos que podem estar causando atrasos.")
        
        # Análise de conformidade
        if aligned_traces:
            fitness_scores = [trace["fitness"] for trace in aligned_traces]
            avg_fitness = sum(fitness_scores) / len(fitness_scores)
            
            if avg_fitness < 0.8:
                suggestions.append(f"Score de conformidade baixo ({avg_fitness:.1%}). Revise os processos que não seguem o padrão esperado.")
            elif avg_fitness > 0.95:
                suggestions.append(f"Excelente conformidade ({avg_fitness:.1%})! Considere usar este processo como modelo para outros departamentos.")
        
        # Análise por categoria de processo
        if 'process_category' in df.columns:
            category_counts = df['process_category'].value_counts()
            for category, count in category_counts.items():
                if count > len(df) * 0.5:  # Mais de 50% dos eventos
                    suggestions.append(f"Categoria '{category}' representa {count/len(df):.1%} dos processos. Foque otimizações nesta área para maior impacto.")
        
        # Análise de recursos
        if 'org:resource' in df.columns:
            resource_counts = df['org:resource'].value_counts()
            if len(resource_counts) > 0:
                busiest_resource = resource_counts.index[0]
                suggestions.append(f"Recurso '{busiest_resource}' está envolvido em {resource_counts[busiest_resource]} atividades. Considere redistribuir carga de trabalho.")
        
        # Análise temporal
        if 'time:timestamp' in df.columns:
            df['hour'] = pd.to_datetime(df['time:timestamp']).dt.hour
            hourly_counts = df['hour'].value_counts().sort_index()
            peak_hour = hourly_counts.idxmax()
            suggestions.append(f"Pico de atividade às {peak_hour}h. Considere balanceamento de carga ou recursos adicionais neste horário.")
        
        # Sugestões padrão se não houver dados suficientes
        if len(suggestions) == 0:
            suggestions = [
                "Implemente monitoramento contínuo dos processos para identificar tendências",
                "Considere criar dashboards em tempo real para acompanhar KPIs de processo",
                "Estabeleça metas de performance para cada etapa do processo"
            ]
    
    except Exception as e:
        suggestions = [f"Erro ao gerar sugestões: {str(e)}"]
    
    return suggestions

@app.route('/api/sankhya-config', methods=['GET', 'POST'])
def sankhya_config():
    """Endpoint para gerenciar configurações da API Sankhya."""
    if request.method == 'GET':
        # Retornar configurações (sem dados sensíveis)
        return jsonify({
            'base_url': SANKHYA_CONFIG.get('base_url', ''),
            'has_app_key': bool(SANKHYA_CONFIG.get('app_key')),
            'has_sankhya_id': bool(SANKHYA_CONFIG.get('sankhya_id')),
            'has_password': bool(SANKHYA_CONFIG.get('password')),
            'has_token': bool(SANKHYA_CONFIG.get('token'))
        })
    
    elif request.method == 'POST':
        # Atualizar configurações (em produção, isso deveria ser mais seguro)
        config_data = request.json
        
        if config_data:
            for key in ['base_url', 'app_key', 'sankhya_id', 'password', 'token']:
                if key in config_data:
                    SANKHYA_CONFIG[key] = config_data[key]
        
        return jsonify({
            'status': 'success',
            'message': 'Configurações atualizadas com sucesso'
        })

@app.route('/api/database-setup', methods=['GET'])
def database_setup_info():
    """Endpoint para fornecer informações sobre setup do banco de dados."""
    return jsonify({
        'view_name': 'VW_PROCESS_MINING_EVENTS',
        'description': 'View para extração de logs de eventos do Sankhya para análise de processos',
        'required_tables': ['TGFCAB', 'TGFTOP', 'TGFITE', 'TGFPRO'],
        'setup_instructions': [
            '1. Execute o script SQL fornecido no banco de dados Sankhya',
            '2. Verifique se o usuário da API tem permissões de leitura na view',
            '3. Teste a view executando: SELECT COUNT(*) FROM VW_PROCESS_MINING_EVENTS',
            '4. Configure as credenciais da API na aplicação'
        ],
        'sql_script_available': True
    })

if __name__ == '__main__':
    # Criar diretório static se não existir
    os.makedirs('static', exist_ok=True)
    os.makedirs('templates', exist_ok=True)
    app.run(debug=True, host='0.0.0.0', port=5001)  # Porta diferente para não conflitar

