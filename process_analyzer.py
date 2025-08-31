import pandas as pd
from pm4py.convert import convert_to_event_log
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments_factory

class ProcessAnalyzer:
    def __init__(self, log_path):
        self.log_path = log_path
        self.event_log = None

    def load_and_convert_log(self):
        """Carrega o log de eventos de um CSV e o converte para o formato PM4Py."""
        print(f"Carregando log de eventos de {self.log_path}...")
        dataframe = pd.read_csv(self.log_path, parse_dates=["timestamp"], dtype={
            'case_id': str,
            'activity': str
        })
        # Renomear colunas para o padrão PM4Py
        dataframe.rename(columns={
            'case_id': 'case:concept:name',
            'activity': 'concept:name',
            'timestamp': 'time:timestamp'
        }, inplace=True)
        self.event_log = convert_to_event_log(dataframe)
        print("Log de eventos carregado e convertido.")
        return self.event_log

    def discover_process_model(self):
        """Descobre o modelo de processo usando o Alpha Miner."""
        if self.event_log is None:
            self.load_and_convert_log()

        print("Descobrindo modelo de processo com Alpha Miner...")
        net, initial_marking, final_marking = alpha_miner.apply(self.event_log)
        print("Modelo de processo descoberto.")
        return net, initial_marking, final_marking

    def visualize_process_model(self, net, initial_marking, final_marking, output_path="process_model.png"):
        """Visualiza o modelo de processo e salva como imagem."""
        print(f"Visualizando modelo de processo e salvando em {output_path}...")
        gviz = pn_visualizer.apply(net, initial_marking, final_marking)
        pn_visualizer.save(gviz, output_path)
        print("Modelo de processo visualizado e salvo.")

    def analyze_conformance(self, net, initial_marking, final_marking):
        """Analisa a conformidade do log de eventos com o modelo de processo."""
        if self.event_log is None:
            self.load_and_convert_log()

        print("Analisando conformidade...")
        aligned_traces = alignments_factory.apply(self.event_log, net, initial_marking, final_marking)
        print(f"Número de traces alinhados: {len(aligned_traces)}")
        num_aligned = sum(1 for trace in aligned_traces if trace["fitness"] == 1.0)
        print(f"Traces com fitness perfeito: {num_aligned}/{len(aligned_traces)}")
        return aligned_traces

    def identify_bottlenecks(self, net, initial_marking, final_marking):
        """Identifica gargalos no processo (exemplo simplificado)."""
        print("Identificando gargalos (exemplo simplificado)...")
        # Coletar todas as atividades do log de eventos
        all_activities = []
        for trace in self.event_log:
            for event in trace:
                all_activities.append(event["concept:name"])

        activity_counts = pd.Series(all_activities).value_counts()
        print("Contagem de atividades:", activity_counts)
        print("Sugestão de otimização: Analisar atividades com alta frequência ou duração para identificar gargalos reais.")

if __name__ == '__main__':
    analyzer = ProcessAnalyzer(log_path="sankhya_processed_data.csv")
    event_log = analyzer.load_and_convert_log()
    net, initial_marking, final_marking = analyzer.discover_process_model()
    analyzer.visualize_process_model(net, initial_marking, final_marking)
    aligned_traces = analyzer.analyze_conformance(net, initial_marking, final_marking)
    analyzer.identify_bottlenecks(net, initial_marking, final_marking)


