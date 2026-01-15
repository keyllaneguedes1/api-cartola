from fastapi import FastAPI
from typing import Optional
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API Brasileirão 2025", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Carregar os dados
try:
    df = pd.read_json("dados_processados_cartola.json") 
    acumulado = pd.read_json("dados_processados_cartola_acumulado.json")  
except Exception as e:
    print("Erro ao carregar dados:", e)
    acumulado = pd.DataFrame()
    df = pd.DataFrame()



# Helper para filtros
def _filtrar(d, clube: Optional[str], posicao: Optional[str], rodada: Optional[int]):
    res = d.copy()
    if clube and "clube" in res.columns:
        res = res[res["clube"] == clube]
    if posicao:
        res = res[res["Posição"] == posicao]
    if rodada:
        res = res[res["atletas.rodada_id"] == rodada]
    return res

# ---------------------------
# Endpoints básicos
# ---------------------------

@app.get("/")
def raiz():
    return {"mensagem": "API do Brasileirão rodando 🚀"}

@app.get("/jogadores")
def listar_jogadores(clube: Optional[str] = None, posicao: Optional[str] = None, nome: Optional[str] = None):
    dados = acumulado.copy()
    if clube and "clube" in dados.columns:
        dados = dados[dados["clube"] == clube]
    if posicao:
        dados = dados[dados["Posição"] == posicao]
    if nome and "atletas.apelido" in dados.columns:
        dados = dados[dados["atletas.apelido"].str.contains(nome, case=False, na=False)]

    return dados.to_dict(orient="records")

@app.get("/jogadores/{id_jogador}")
def jogador_info(id_jogador: int):
    dados = acumulado[acumulado["atletas.atleta_id"] == id_jogador]
    return dados.to_dict(orient="records")

@app.get("/jogadores/{id_jogador}/rodadas")
def jogador_rodadas(id_jogador: int, limite: int = 5):
    dados = df[df["atletas.atleta_id"] == id_jogador].sort_values("atletas.rodada_id", ascending=False)
    return dados.head(limite).to_dict(orient="records")

@app.get("/ranking/rodada")
def ranking_rodada(rodada: int, posicao: Optional[str] = None, limite: int = 10):
    # Filtra pela rodada
    dados = df[df["atletas.rodada_id"] == rodada]
    
    # Se quiser filtrar por posição
    if posicao:
        dados = dados[dados["Posição"] == posicao]
    
    # Ordena pela pontuação fantasy
    ranking = dados.sort_values("pontos_fantasy", ascending=False).head(limite)
    
    # Retorna apenas colunas úteis
    return ranking[["atletas.atleta_id", "atletas.apelido", "Posição", "pontos_fantasy"]].to_dict(orient="records")


@app.get("/comparacao")
def comparacao(id_jogador1: int, id_jogador2: int):
    j1 = acumulado[acumulado["atletas.atleta_id"] == id_jogador1].to_dict(orient="records")
    j2 = acumulado[acumulado["atletas.atleta_id"] == id_jogador2].to_dict(orient="records")
    return {"jogador1": j1, "jogador2": j2}

@app.get("/estatisticas/clube/{clube}")
def estatisticas_clube(clube: str):
    dados = acumulado[acumulado["clube"] == clube]
    return dados.to_dict(orient="records")

# ---------------------------
# Endpoints de scouts
# ---------------------------

@app.get("/scouts/ataque/top-assistencias")
def top_assistencias(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["A"].sum().sort_values("A", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/defesa/top-desarmes")
def top_desarmes(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["DS"].sum().sort_values("DS", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/ataque/top-gols")
def top_gols(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["G"].sum().sort_values("G", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/ataque/top-finalizacoes-perigosas")
def top_finalizacoes_perigosas(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    if "Finalizacoes_Perigosas" not in dados.columns:
        dados["Finalizacoes_Perigosas"] = dados["FD"] + dados["FT"]
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["Finalizacoes_Perigosas"].sum().sort_values("Finalizacoes_Perigosas", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/ataque/top-faltas-sofridas")
def top_faltas_sofridas(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["FS"].sum().sort_values("FS", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/defesa/top-faltas-cometidas")
def top_faltas_cometidas(rodada: Optional[int] = None, limite: int = 10, posicao: Optional[str] = None, clube: Optional[str] = None):
    dados = _filtrar(df, clube, posicao, rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["FC"].sum().sort_values("FC", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/goleiros/top-defesas-dificeis")
def top_defesas_dificeis(rodada: Optional[int] = None, limite: int = 10, clube: Optional[str] = None):
    dados = _filtrar(df, clube, "GOL", rodada)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["DE"].sum().sort_values("DE", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/goleiros/top-penaltis-defendidos")
def top_penaltis_defendidos(limite: int = 10, clube: Optional[str] = None):
    dados = _filtrar(df, clube, "GOL", None)
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["DP"].sum().sort_values("DP", ascending=False).head(limite)
    return ranking.to_dict(orient="records")

@app.get("/scouts/defesa/top-jogos-sem-gol")
def top_jogos_sem_gol(rodada: Optional[int] = None, limite: int = 10, clube: Optional[str] = None):
    dados = _filtrar(df, clube, None, rodada)
    dados = dados[dados["Posição"].isin(["GOL","ZAG","LAT"])]
    ranking = dados.groupby(["atletas.atleta_id","atletas.apelido"], as_index=False)["SG"].sum().sort_values("SG", ascending=False).head(limite)
    return ranking.to_dict(orient="records")


# ---------------------------
# Endpoints de gráficos
# ---------------------------
@app.get("/graficos/top-pontuadores")
def top_pontuadores(limite: int = 20):
    ranking = acumulado.sort_values("pontos_fantasy", ascending=False).head(limite)
    return ranking[["atletas.apelido","Posição","pontos_fantasy"]].to_dict(orient="records")

@app.get("/graficos/gols-vs-assistencias")
def gols_vs_assistencias():
    dados = acumulado[["atletas.apelido","G","A"]]
    return dados.to_dict(orient="records")

@app.get("/graficos/evolucao-top3")
def evolucao_top3(limite_rodadas: int = 5):
    top3 = acumulado.sort_values("pontos_fantasy", ascending=False).head(3)["atletas.atleta_id"].tolist()
    dados = df[df["atletas.atleta_id"].isin(top3)].sort_values("atletas.rodada_id", ascending=True)
    return dados[["atletas.apelido","atletas.rodada_id","pontos_fantasy"]].to_dict(orient="records")

@app.get("/jogadores/{id_jogador}/scouts-detalhado")
def jogador_scouts_detalhado(id_jogador: int):
    dados_jogador = df[df["atletas.atleta_id"] == id_jogador]
    
    if dados_jogador.empty:
        return {"erro": "Jogador não encontrado"}
    
    total_rodadas = len(dados_jogador)
    
    # Totais
    totais = {
        "G": int(dados_jogador["G"].sum()),
        "A": int(dados_jogador["A"].sum()),
        "DS": int(dados_jogador["DS"].sum()),
        "FC": int(dados_jogador["FC"].sum()),
        "FS": int(dados_jogador["FS"].sum()),
        "FD": int(dados_jogador["FD"].sum()),
        "FT": int(dados_jogador["FT"].sum()),
        "DE": int(dados_jogador["DE"].sum()),
        "DP": int(dados_jogador["DP"].sum()),
        "SG": int(dados_jogador["SG"].sum()),
    }
    
    # Médias por rodada
    medias = {f"{k}_media": v/total_rodadas for k, v in totais.items()}
    
    # Percentual de jogos com contribuição
    percentuais = {}
    for scout in ["G", "A", "DS"]:
        jogos_com_scout = len(dados_jogador[dados_jogador[scout] > 0])
        percentuais[f"{scout}_frequencia"] = f"{(jogos_com_scout/total_rodadas)*100:.1f}%"
    
    return {
        "totais": totais,
        "medias": medias,
        "percentuais": percentuais,
        "total_rodadas": total_rodadas,
        "posicao": dados_jogador.iloc[0]["Posição"] if "Posição" in dados_jogador.columns else None
    }