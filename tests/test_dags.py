from  pathlib import Path
import pytest
from airflow.models import DagBag

def test_ONS_energia():
    dagdir =(Path(__file__).parents[1] / "dags").resolve() # Caminho para o diretório das DAGs absoluto
    assert dagdir.exists(), f"Diretório das DAGs não encontrado: {dagdir}"  # Valida se o diretório das DAGs existe
    dagbag = DagBag(dag_folder=str(dagdir))

    assert (
        len(dagbag.import_errors) == 0  #confirma se não houve erros de importação das DAGs
    ), f"Erros de importação encontrados: {dagbag.import_errors}"  
       
    num_dags = len(dagbag.dags) # Conta o número de DAGs importadas
    min_dags = 1  # Defina o número mínimo de DAGs esperadas, tem que ser maior que 0 pois so tem apenas uma DAG no projeto  
    #Validador de quantidade de DAGs
    assert(num_dags >= min_dags), f"Quantidade de DAGs insuficiente: {num_dags} < {min_dags}"


@pytest.fixture(scope="module")
def dagbag():
    dagdir = (Path(__file__).parents[1] / "dags").resolve()
    return DagBag(dag_folder=str(dagdir))


TASKS_ESPERADAS = {
    "extrair_dados",
    "carregar_dados",
    "transformar_dados",
    "auditoria_dados",
    "tabela_geração_dados",
    "inserir_dados_tabela",
}


def test_ons_energia_tem_as_tasks_esperadas(dagbag):
    dag = dagbag.dags["ONS_energia"]
    assert set(dag.task_ids) == TASKS_ESPERADAS, f"Tasks encontradas: {sorted(dag.task_ids)}"


@pytest.mark.parametrize(
    "upstream, downstream",
    [
        ("extrair_dados", "carregar_dados"),
        ("carregar_dados", "transformar_dados"),
        ("transformar_dados", "auditoria_dados"),
        ("auditoria_dados", "inserir_dados_tabela"),
        ("tabela_geração_dados", "inserir_dados_tabela"),
    ],
)
def test_ons_energia_dependencias(dagbag, upstream, downstream):
    dag = dagbag.dags["ONS_energia"]
    assert upstream in dag.get_task(downstream).get_flat_relative_ids(upstream=True), (
        f"{downstream} deveria rodar depois de {upstream}"
    )


@pytest.fixture(scope="module")
def transformar(dagbag):
    return dagbag.dags["ONS_energia"].get_task("transformar_dados").python_callable


def linha(subsistema, hidraulica=0, termica=0, eolica=0, solar=0):
    return {
        "nom_subsistema": subsistema,
        "val_gerhidraulica": hidraulica,
        "val_gertermica": termica,
        "val_gereolica": eolica,
        "val_gersolar": solar,
    }


def test_pct_renovavel_agrega_antes_de_calcular_o_percentual(transformar):
    # linha A: 100% renovável (total 100); linha B: 0% renovável (total 300)
    resultado = transformar([linha("SUL", hidraulica=100), linha("SUL", termica=300)])
    assert len(resultado) == 1
    # médias: renovável 50 / total 200 = 25%. Média dos percentuais daria 50%, que seria errado
    assert resultado[0]["pct_renovavel"] == pytest.approx(25.0)


def test_pct_renovavel_gera_um_registro_por_subsistema(transformar):
    resultado = transformar([
        linha("SUL", hidraulica=50, termica=50),
        linha("NORTE", eolica=30, solar=10, termica=60),
    ])
    pct = {r["nom_subsistema"]: r["pct_renovavel"] for r in resultado}
    assert pct == pytest.approx({"SUL": 50.0, "NORTE": 40.0})

