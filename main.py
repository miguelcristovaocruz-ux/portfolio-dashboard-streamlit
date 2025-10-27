# main.py
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.express as px

ticker = "PETR4.SA"

# Baixa 1 ano de dados diários
dados = yf.download(ticker, period="1y")

# Se vier com MultiIndex, pega só a coluna 'Close'
if isinstance(dados.columns, pd.MultiIndex):
    dados.columns = dados.columns.droplevel(1)  # remove o nível do ticker

# Agora a coluna 'Close' é simples
dados["Retorno"] = dados["Close"].pct_change()
retorno_medio = dados["Retorno"].mean() * 252
vol_anual = dados["Retorno"].std() * np.sqrt(252)

print(f"Retorno médio anual: {retorno_medio:.2%}")
print(f"Volatilidade anualizada: {vol_anual:.2%}")

# Gráfico interativo
fig = px.line(dados, y="Close", title=f"Preço ajustado - {ticker}")
fig.show()
