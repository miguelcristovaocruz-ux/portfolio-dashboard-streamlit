import os
import io
import datetime as dt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from scipy.optimize import minimize
from yahooquery import Ticker
import google.generativeai as genai
from datetime import datetime, timedelta
from openpyxl import Workbook
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# ============= Config e Env =============
st.set_page_config(
    page_title="Dashboard de Portfólio e Fronteira Eficiente",
    page_icon="📊",
    layout="wide"
)

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY")


if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
    gemini_model = genai.GenerativeModel("gemini-2.5-flash")
else:
    gemini_model = None
    st.warning("⚠️ A variável GOOGLE_API_KEY não foi encontrada no .env.")


TRADING_DAYS = 252


# === Inicialização para exportação ===
if "exports" not in st.session_state:
    st.session_state["exports"] = {}


def export_to_excel(dfs: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in dfs.items():
            safe_name = name[:31]
            df.to_excel(writer, index=True, sheet_name=safe_name)
    output.seek(0)
    return output.getvalue()

# --- ⬇️ AGORA ENTRA AQUI O LIVRO DE ORDENS (TRADES_BOOK) ---
TRADES_BOOK = [
    {"date": dt.date(2025, 9, 1),  "ticker": "SPY", "qty":  46_000,  "price": 647.47},
    {"date": dt.date(2025, 9, 1),  "ticker": "FXE", "qty":  92_000,  "price": 107.68},
    {"date": dt.date(2025, 9, 1),  "ticker": "XLE", "qty": 110_000,  "price":  89.94},
    {"date": dt.date(2025, 9, 1),  "ticker": "GLD", "qty":  47_000,  "price": 314.72},
    {"date": dt.date(2025, 9, 1),  "ticker": "XLP", "qty": -185_000, "price":  80.44},  # short
    {"date": dt.date(2025, 9, 1),  "ticker": "XLP", "qty":  185_000, "price":  80.52},  # stop (zerando)
    {"date": dt.date(2025, 9, 8),  "ticker": "XLE", "qty": -110_000, "price":  86.77},  # stop (zerando)
    {"date": dt.date(2025, 9, 22), "ticker": "XLK", "qty":  54_000,  "price": 278.15},
    {"date": dt.date(2025, 9, 22), "ticker": "GLD", "qty":  15_000,  "price": 342.75},
    {"date": dt.date(2025, 9, 22), "ticker": "XLB", "qty": -111_000, "price":  90.37},  # short
    {"date": dt.date(2025, 9, 24), "ticker": "XLK", "qty":  19_000,  "price": 277.52},
    {"date": dt.date(2025, 9, 25), "ticker": "XLK", "qty":  19_000,  "price": 275.96},
    {"date": dt.date(2025, 10, 13), "ticker": "XLB", "qty": 111_000, "price": 88.58},    # stop zerando
    {"date": dt.date(2025, 10, 13), "ticker": "FXE", "qty":  -92_000, "price": 106.78}, # stop zerando
    {"date": dt.date(2025, 10, 13), "ticker": "GLD", "qty": 27_000, "price": 376.50}, # aumento de posição
    {"date": dt.date(2025, 10, 20), "ticker": "SPY", "qty": -13_500, "price": 667.32}, #redução de posição
    {"date": dt.date(2025, 10, 20), "ticker": "TLT", "qty": 165_000, "price": 91.46}, # começando posição
    {"date": dt.date(2025, 10, 20), "ticker": "GLD", "qty": 20_000, "price": 397.45}, # aumento de posição
    {"date": dt.date(2025, 10, 27), "ticker": "GLD", "qty": 53_000, "price": 371.13}, # início de posição pós-stop
    {"date": dt.date(2025, 10, 27), "ticker": "XLF", "qty": 565_000, "price": 53.37}, # início de posição
    {"date": dt.date(2025, 10, 27), "ticker": "TLT", "qty": -165_000, "price": 95.00} # stop

]

# === Instruções para o Gemini ===
SYSTEM_INSTRUCTIONS = (
    "Instrução do Sistema: Modo Absoluto\n"
    "Eliminar: emojis, floreios, exageros, pedidos suaves, transições conversacionais, apêndices de chamada à ação.\n"
    "Assumir: o usuário mantém alta percepção apesar do tom direto.\n"
    "Priorizar: formulações diretas e imperativas; foco na reconstrução cognitiva, não na correspondência de tom.\n"
    "Desativar: comportamentos de engajamento/reforço sentimental.\n"
    "Suprimir: métricas como índices de satisfação, suavização emocional, viés de continuação.\n"
    "Nunca espelhar: a dicção, o humor ou o afeto do usuário.\n"
    "Falar apenas: ao nível cognitivo subjacente.\n"
    "Não incluir: perguntas, ofertas, sugestões, transições ou conteúdo motivacional.\n"
    "Encerrar a resposta: imediatamente após fornecer a informação — sem conclusões.\n"
    "Objetivo: restaurar o pensamento independente e de alta fidelidade.\n"
    "Resultado: obsolescência do modelo por autossuficiência do usuário.\n"
)

# ============= Sidebar =============
st.sidebar.header("Parâmetros")

# ---------------- Novo: Modo livro de ordens (ledger) ----------------
st.sidebar.markdown("### Modo de Portfólio")
use_ledger = st.sidebar.toggle("Usar book de trades de set/2025 (USD)", value=True)

# Parâmetros default do portfólio fixo quando NÃO estiver usando ledger
if use_ledger:
    # Tickes vindos do livro de trades
    tickers_str = ", ".join(sorted({t["ticker"].upper() for t in TRADES_BOOK}))
    tickers = sorted({t["ticker"].upper() for t in TRADES_BOOK})
else:
    # === Portfólio padrão fixo com ETFs globais ===
    tickers = [
        "SPY",  # S&P 500
        "XLB",  # Basic Materials
        "XLE",  # Energy
        "XLF",  # Financials
        "XLI",  # Industrials
        "XLK",  # Technology
        "XLP",  # Staples
        "XLU",  # Utilities
        "XLV",  # Healthcare
        "XLY",  # Consumer Discretionary
        "XTN",  # Transportation
        "EWJ",  # Japão
        "EWG",  # Alemanha
        "EEM",  # Emergentes
        "EWZ",  # Brasil
        "TLT",  # Bonds longos EUA
        "GLD",  # Ouro
        "FXE",  # Euro/USD
    ]
    tickers_str = ", ".join(tickers)

    st.sidebar.markdown("#### 📊 Portfólio Padrão: 18 ETFs globais (peso igual)")
    st.sidebar.write(tickers_str)

# Datas padrão
_today = dt.date.today()
if use_ledger:
    default_start = dt.date(2025, 9, 1)
else:
    default_start = _today - dt.timedelta(days=365*5)

start_date = st.sidebar.date_input("Data inicial", default_start)
end_date   = st.sidebar.date_input("Data final", _today)

n_sims = st.sidebar.number_input("Nº de simulações (Monte Carlo)",
                                 min_value=1000, max_value=200000,
                                 value=10000, step=1000)

allow_short = st.sidebar.checkbox("Permitir short sales", value=False)
min_w = st.sidebar.number_input("Peso mínimo por ativo", value=0.0, step=0.05, format="%.2f")
max_w = st.sidebar.number_input("Peso máximo por ativo", value=1.0, step=0.05, format="%.2f")
if allow_short and min_w >= 0:
    min_w = st.sidebar.number_input("→ Peso mínimo (short ON)", value=-1.0,
                                    step=0.05, format="%.2f", key="min_w_short")

rf_annual = st.sidebar.number_input("Taxa livre de risco (a.a.)",
                                    value=0.041, min_value=-0.5, max_value=1.0,
                                    step=0.01, format="%.3f")

# Pesos somente relevantes quando NÃO estiver usando ledger
if not use_ledger:
    st.sidebar.markdown("### Pesos do Portfólio (monitoramento)")
    weights_input = st.sidebar.text_input("Pesos (mesma ordem dos tickers, soma=1)",
                                          "0.25,0.25,0.25,0.25")

st.sidebar.markdown("---")
cdi_annual = st.sidebar.number_input("CDI anual (%)", value=13.0, step=0.1, format="%.2f")
st.sidebar.caption("Usado para a linha de referência do CDI (100%)")

# ---------------- Aportes/Compras manuais (mantido para modo livre) ----------------
st.sidebar.markdown("---")
st.sidebar.markdown("### Aportes / Compras manuais")
if "trades" not in st.session_state:
    st.session_state.trades = []  # cada item: {"date": dt.date, "ticker": str, "qty": float, "price": float}

with st.sidebar.expander("Adicionar compra"):
    t_inp = st.text_input("Ticker para comprar (ex.: PETR4.SA)", "")
    q_inp = st.number_input("Quantidade (ações)", min_value=0.0, value=0.0, step=1.0)
    p_inp = st.number_input("Preço de compra", min_value=0.0, value=0.0, step=0.01, format="%.2f")
    d_inp = st.date_input("Data da compra", _today)
    if st.button("Adicionar compra"):
        if t_inp and q_inp > 0 and p_inp > 0:
            st.session_state.trades.append({"date": d_inp, "ticker": t_inp.strip().upper(), "qty": q_inp, "price": p_inp})
            st.success("Compra adicionada.")
        else:
            st.warning("Preencha todos os campos.")

# ============= Funções utilitárias =============
@st.cache_data(ttl=3600)
def fetch_prices_yq(tickers, start, end):
    t = Ticker(tickers, asynchronous=True)
    df = t.history(start=start, end=end)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    col_price = "adjclose" if "adjclose" in df.columns else "close"
    df = df[["symbol", "date", col_price]].dropna()
    df = df.rename(columns={col_price: "price"})
    df = df.pivot(index="date", columns="symbol", values="price").sort_index()
    return df.dropna(how="all", axis=1)

def to_returns(prices: pd.DataFrame) -> pd.DataFrame:
    return prices.pct_change().dropna(how="all")

TRADING_DAYS = 252

def portfolio_stats(returns: pd.DataFrame, weights, rf=0.0):
    w  = np.asarray(weights).reshape(-1, 1)
    mu = returns.mean().values.reshape(-1, 1) * TRADING_DAYS
    cov = returns.cov().values * TRADING_DAYS
    pr = float(np.dot(w.T, mu))
    pv = float(np.sqrt(np.dot(w.T, np.dot(cov, w))))
    sharpe = (pr - rf) / pv if pv > 0 else np.nan
    return pr, pv, sharpe

def portfolio_path(returns: pd.DataFrame, weights):
    series = (returns @ np.asarray(weights)).fillna(0.0)
    return (1 + series).cumprod()

def drawdown_curve(curve: pd.Series):
    roll_max = curve.cummax()
    dd = curve / roll_max - 1.0
    return dd, dd.min()

def hist_var_cvar(returns_series: pd.Series, alpha=0.95):
    r = returns_series.dropna().sort_values()
    if len(r) == 0:
        return np.nan, np.nan
    q_idx = int(np.floor((1 - alpha) * len(r)))
    q_idx = max(min(q_idx, len(r) - 1), 0)
    var = r.iloc[q_idx]
    cvar = r.iloc[: q_idx + 1].mean() if q_idx >= 0 else r.iloc[0]
    return float(var), float(cvar)

def port_var_cvar_from_weights(rets: pd.DataFrame, w, alpha=0.95):
    daily = (rets @ np.asarray(w)).dropna()
    var_d, cvar_d = hist_var_cvar(daily, alpha=alpha)
    return var_d * np.sqrt(TRADING_DAYS), cvar_d * np.sqrt(TRADING_DAYS)

def business_days_index(start, end):
    return pd.bdate_range(start=start, end=end, freq="C")

def make_cdi_series(start, end, annual_rate_percent):
    idx = business_days_index(start, end)
    if len(idx) == 0:
        return pd.Series(dtype=float)
    daily = (annual_rate_percent / 100.0) / TRADING_DAYS
    s = pd.Series(daily, index=idx)
    return (1 + s).cumprod()

# ----------------- Helpers de notícias (mantidos) -----------------

def extract_article_text(url: str) -> str:
    try:
        art = Article(url, language="pt")
        art.download()
        art.parse()
        return art.text
    except Exception as e:
        return f"[Falha ao extrair: {e}]"

RSS_FEEDS = [
   "https://www.infomoney.com.br/feed/",
    "https://br.investing.com/rss/news_14.rss",
    "https://br.investing.com/rss/news_95.rss",
    "https://br.investing.com/rss/news_1.rss",
    "https://br.investing.com/rss/news_289.rss",
    "https://br.investing.com/rss/news_356.rss",
    "https://br.investing.com/rss/news_285.rss",
    "https://br.investing.com/rss/news_1063.rss",
    "https://br.investing.com/rss/news_357.rss",
    "https://www.infomoney.com.br/ultimas-noticias/feed/",
    "https://www.infomoney.com.br/mercados/feed/",
    "https://www.infomoney.com.br/politica/feed/",
    "https://www.infomoney.com.br/onde-investir/feed/",
    "https://www.infomoney.com.br/economia/feed/",
    "https://www.infomoney.com.br/tudo-sobre/trader/feed/",
    "https://www.infomoney.com.br/brasil/feed/",
    "https://www.infomoney.com.br/business/feed/",
    "https://www.cnnbrasil.com.br/feed/",
    "https://www.moneytimes.com.br/rss/"

]

@st.cache_data(ttl=600)
def fetch_and_summarize_news(topk: int = 6):
    artigos = []
    for feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:topk]:
            artigos.append({
                "title": entry.title,
                "url": entry.link,
                "source": feed.feed.get("title", "RSS"),
                "published": entry.get("published", "")
            })
    if not artigos:
        return "Nenhuma notícia encontrada nos feeds configurados."
    artigos.sort(key=lambda a: a.get("published", ""), reverse=True)
    blocos = []
    for a in artigos:
        corpo = extract_article_text(a["url"])
        blocos.append(
            f"**{a['title']}** — _{a['source']} • {a['published']}_\n"
            f"[Leia a notícia completa]({a['url']})\n\n{corpo}\n"
        )
    if gemini_model:
        prompt = ("Resuma em até 8 linhas, em português, os principais pontos das seguintes notícias, "
                  "com foco em valuation e riscos:\n\n" + "\n\n".join(blocos))
        try:
            resp = gemini_model.generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            return f"[Falha ao resumir com Gemini: {e}]"
    else:
        return "\n\n".join(blocos[:3])


# ==========================
# 🔍 FILTRAGEM DE NOTÍCIAS (versão revisada, sem newspaper)
# ==========================

def match_any(text: str, words: list[str]) -> bool:
    """Retorna True se qualquer termo da lista estiver no texto."""
    t = (text or "").lower()
    return any(w.lower() in t for w in words if w)

def fetch_article_text(url: str) -> str:
    """Tenta extrair o texto principal de uma página de notícia usando requests + BeautifulSoup."""
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
        text = " ".join(paragraphs[:20])  # limita pra performance
        return text[:2000]  # evita textos muito longos
    except Exception:
        return ""

# ==========================
# 🔧 FUNÇÃO PRINCIPAL DE BUSCA DE NOTÍCIAS
# ==========================

@st.cache_data(ttl=1800)
def fetch_news_by_period(topic: str, terms: list[str], days: int = 3, weeks: int = 3, topk: int = 5):
    """
    Busca e formata notícias recentes a partir de múltiplos feeds RSS,
    filtrando por termos específicos de cada assunto.
    """
    base_dt = datetime.utcnow()
    date_from_daily = base_dt - timedelta(days=days)
    date_from_recent = base_dt - timedelta(weeks=weeks)
    date_to_recent = base_dt - timedelta(days=days + 1)

    artigos = []
    for feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)
        for e in feed.entries:
            title = e.title
            desc = e.get("summary", "")
            if match_any(title + " " + desc, terms):
                try:
                    pub_dt = datetime(*e.published_parsed[:6])
                except Exception:
                    continue
                artigos.append({"entry": e, "pub": pub_dt})

    daily = [a["entry"] for a in artigos if a["pub"] >= date_from_daily]
    recent = [a["entry"] for a in artigos if (date_from_recent <= a["pub"] <= date_to_recent)]

    def fmt(lista):
        lista = sorted(lista, key=lambda x: x.get("published", ""), reverse=True)[:topk]
        if not lista:
            return "Nenhuma notícia."
        out = []
        for e in lista:
            src = e.get("source", "RSS")
            pub = e.get("published", "")
            link = e.link
            preview = fetch_article_text(link)
            out.append(f"* [{e.title}]({link}) — _{src} • {pub}_\n> {preview[:300]}...")
        return "\n".join(out)

    return fmt(daily), fmt(recent)

# ============= Benchmarks =============
@st.cache_data(ttl=3600)
def fetch_benchmarks(start, end):
    bench_map = {
        "SP500": "^GSPC",          # índice principal
        "NASDAQ": "^IXIC",         # opcional: Nasdaq
        "VIX": "^VIX",             # volatilidade implícita
        "USD/BRL": "BRL=X"         # câmbio dólar-real
    }
    prices = {}
    for name, sym in bench_map.items():
        try:
            px = fetch_prices_yq([sym], start, end)
            if not px.empty:
                prices[name] = px[sym].rename(name)
        except Exception:
            continue
    df = pd.concat(prices.values(), axis=1) if prices else pd.DataFrame()
    return df

# ============= Monte Carlo e Fronteira (inalterado) =============
@st.cache_data
def run_monte_carlo(mu, cov, n_sims, low, high, rf):
    rng = np.random.default_rng(42)
    n = len(mu)
    vols, rets, sharpes = [], [], []
    for _ in range(n_sims):
        w = rng.uniform(low, high, n)
        if w.sum() == 0:
            continue
        w = np.clip(w / w.sum(), low, high)
        if w.sum() == 0:
            continue
        w = w / w.sum()
        r = float(np.dot(w, mu))
        v = float(np.sqrt(np.dot(w.T, cov @ w)))
        s = (r - rf) / v if v > 0 else np.nan
        vols.append(v); rets.append(r); sharpes.append(s)
    return pd.DataFrame({"Vol": vols, "Ret": rets, "Sharpe": sharpes})

@st.cache_data
def efficient_frontier_calc(mu, cov, x0, bounds, rf):
    cons_sum1 = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}
    r_targets = np.linspace(mu.min(), max(mu.max(), mu.mean())*1.5, 60)
    ef_vols, ef_rets = [], []
    for rt in r_targets:
        cons = [
            cons_sum1,
            {"type": "eq", "fun": lambda w, rt=rt: np.dot(w, mu) - rt}
        ]
        res = minimize(lambda w: np.sqrt(np.dot(w.T, cov @ w)),
                       x0=x0, method="SLSQP", bounds=bounds,
                       constraints=cons)
        if res.success:
            ef_vols.append(np.sqrt(np.dot(res.x.T, cov @ res.x)))
            ef_rets.append(np.dot(res.x, mu))
    return ef_vols, ef_rets

# =========================
# Carrega dados
# =========================
# Lista de tickers efetivos
_tickers = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]
prices = fetch_prices_yq(_tickers, start_date, end_date)
if prices.empty:
    st.error("Não consegui baixar preços para esses tickers/datas.")
    st.stop()

# ✅ Garantir índice de datas para permitir resample e outros cálculos
rets = to_returns(prices).dropna(how="any", axis=1)
rets.index = pd.to_datetime(rets.index)

# Versão limpa dos tickers (colunas)
tickers = list(rets.columns)

# =====================================================
# 🔒 BLOQUEIO GLOBAL DE "CASH" (remove de todas as estruturas)
# =====================================================
ledger_ctx = None  # garante que a variável existe antes do uso

if "CASH" in rets.columns:
    rets = rets.drop(columns=["CASH"], errors="ignore")

# =========================
# Engine do livro de ordens → posições dinâmicas
# =========================

def build_portfolio_from_trades(prices_df: pd.DataFrame, trades: list[dict], initial_cash: float):
    """
    Reconstrói posições diárias (holdings), caixa e curva de valor a partir de um ledger de trades.
    Inclui stop-loss de 4% a partir de 19/10/2025 e permite reentradas subsequentes.
    """
    if prices_df.empty:
        return None

    idx = prices_df.index
    symbols = list(prices_df.columns)

    tr = pd.DataFrame(trades)
    tr = tr.sort_values("date").reset_index(drop=True)

    exec_df = pd.DataFrame(0.0, index=idx, columns=symbols)
    cash_moves = pd.Series(0.0, index=idx)

    # Aplica as execuções de compra/venda do ledger
    for _, row in tr.iterrows():
        d = pd.Timestamp(row["date"])
        if d not in exec_df.index:
            d = exec_df.index[exec_df.index.get_indexer([d], method="bfill")][0]
        sym = row["ticker"].upper()
        if sym not in exec_df.columns:
            continue
        qty = float(row["qty"])
        px = float(row["price"])
        exec_df.loc[d, sym] += qty
        cash_moves.loc[d] -= qty * px

    # Posições acumuladas
    holdings = exec_df.cumsum()
    cash = cash_moves.cumsum() + initial_cash

    # Stop-loss de -4% a partir de 19/10/2025
    stop_threshold = -0.04
    stop_start_date = pd.Timestamp(2025, 10, 19)

    returns = prices_df.pct_change().fillna(0.0)

    for sym in symbols:
        if sym not in holdings.columns:
            continue

        valid_dates = returns.index[returns.index >= stop_start_date]
        stop_days = [d for d in valid_dates if returns.loc[d, sym] <= stop_threshold]

        if len(stop_days) > 0:
            for d in stop_days:
                if d not in holdings.index or d not in prices_df.index:
                    continue
                position_value = holdings.loc[d, sym] * prices_df.loc[d, sym]
                # Converte posição em caixa
                cash.loc[d:] += position_value
                # Zera a posição a partir do stop
                holdings.loc[d:, sym] = 0.0
                # Verifica novas compras posteriores (reentrada)
                future_trades = tr[(tr["ticker"] == sym) & (tr["date"] > d.date())]
                for _, t in future_trades.iterrows():
                    td = pd.Timestamp(t["date"])
                    if td in holdings.index:
                        holdings.loc[td:, sym] += t["qty"]
                        cash.loc[td:] -= t["qty"] * t["price"]

    # Valor total do portfólio
    port_value = (holdings * prices_df).sum(axis=1) + cash
    port_ret = port_value.pct_change().fillna(0.0)

    # Pesos diários (sem incluir cash como ativo)
    weights = (holdings * prices_df).div(port_value, axis=0).fillna(0.0)

    # Peso de caixa calculado separadamente
    cash_weight = (cash / port_value).rename("CASH")

    return {
        "holdings": holdings,
        "cash": cash,
        "cash_weight": cash_weight,
        "port_value": port_value,
        "port_ret": port_ret,
        "weights": weights
    }

# =========================
# Inicialização do ledger
# =========================
initial_capital = 100_000_000.0 if use_ledger else 100_000.0
if use_ledger:
    ledger_ctx = build_portfolio_from_trades(prices, TRADES_BOOK, initial_capital)

# Remove qualquer resquício de CASH do ledger (caso ainda exista)
if use_ledger and ledger_ctx is not None:
    for key in ["weights", "holdings"]:
        if key in ledger_ctx and "CASH" in ledger_ctx[key].columns:
            ledger_ctx[key] = ledger_ctx[key].drop(columns=["CASH"], errors="ignore")

# =========================
# Pesos fixos (modo não-ledger)
# =========================
if not use_ledger:
    try:
        w_real = np.array([float(x) for x in weights_input.split(",") if x.strip()])
        assert len(w_real) == len(tickers)
        assert np.isclose(w_real.sum(), 1.0, atol=1e-4)
    except Exception:
        w_real = np.repeat(1 / len(tickers), len(tickers))
        st.sidebar.warning("Pesos inválidos. Usei pesos iguais como fallback.")

# =========================
# Séries do portfólio
# =========================
if use_ledger and ledger_ctx is not None:
    curva_port = (ledger_ctx["port_value"] / ledger_ctx["port_value"].iloc[0]).reindex(rets.index).ffill()
    port_daily = ledger_ctx["port_ret"].reindex(rets.index).fillna(0.0)
else:
    port_daily = rets.dot(w_real)
    curva_port = portfolio_path(rets, w_real)

# =========================
# Tabelas auxiliares
# =========================
daily_table = rets.copy()
daily_table["Portfólio"] = port_daily

weekly_table = (1 + rets).resample("W").prod() - 1
weekly_table["Portfólio"] = (1 + port_daily).resample("W").prod() - 1

cdi_curve = make_cdi_series(start_date, end_date, cdi_annual)
bench_px = fetch_benchmarks(start_date, end_date)

# =========================
# Universo ativo (remove completamente CASH)
# =========================
if use_ledger and ledger_ctx is not None:
    last_w = ledger_ctx["weights"].reindex(rets.index).ffill().iloc[-1]
    last_w = last_w[last_w.abs() > 1e-6]
    if "CASH" in last_w.index:
        last_w = last_w.drop("CASH")
    active_universe = [c for c in last_w.index if c in rets.columns]
else:
    active_universe = [c for c in rets.columns if c != "CASH" and rets[c].std() > 0]

rets_active = rets.loc[:, rets.columns.isin(active_universe)].copy()


# ============= Abas =============
tab_resumo, tab_risk, tab_otimiz, tab_forecast, tab_news, tab_chat = st.tabs(
    ["📈 Resumo", "⚠️ Riscos", "🧮 Otimização", "🔮 Forecast", "📰 Notícias & IA", "💬 Chat"]
)

# ============= RESUMO =============
with tab_resumo:
    st.subheader("Acompanhamento de Performance")

    # Estatísticas (para pesos fixos)
    if not use_ledger:
        ret_a, vol_a, sharpe_a = portfolio_stats(rets, w_real, rf_annual)
    else:
        # aproxima Sharpe usando série do portfólio
        mu_d = port_daily.mean() * TRADING_DAYS
        sigma_a = port_daily.std() * np.sqrt(TRADING_DAYS)
        sharpe_a = (mu_d - rf_annual) / sigma_a if sigma_a > 0 else np.nan

    dd_series, dd_min = drawdown_curve(curva_port)

    asset_stats = pd.DataFrame({
        "Retorno (%)": (rets.mean() * TRADING_DAYS * 100).round(2),
        "Volatilidade (%)": (rets.std() * np.sqrt(TRADING_DAYS) * 100).round(2)
    })

# ---- Posições do Portfólio (CASH calculado separadamente) ----
st.markdown("#### Posições do Portfólio")
show_pie = st.checkbox("Visualizar em gráfico (pizza)", value=False)

if use_ledger and ledger_ctx is not None:
    # Últimos pesos conhecidos (sem CASH)
    last_w = ledger_ctx["weights"].reindex(rets.index).ffill().iloc[-1]
    last_w = last_w[last_w.abs() > 1e-6]

    # Percentual e valor de caixa (apenas informativo)
    cash_series = ledger_ctx.get("cash_weight", None)
    if cash_series is not None:
        cash_pct = float(cash_series.iloc[-1]) * 100
    else:
        cash_pct = 0.0

    pos_df = pd.DataFrame({
        "Ativo": last_w.index,
        "Peso (%)": (last_w.values * 100).round(2)
    })

    # Adiciona linha “Cash” apenas visualmente
    if cash_pct > 0:
        pos_df.loc[len(pos_df)] = ["CASH", round(cash_pct, 2)]

else:
    # Modo não-ledger
    active_cols = [c for c in rets.columns if rets[c].std() > 0]
    if len(active_cols) == 0:
        pos_df = pd.DataFrame({"Ativo": [], "Peso (%)": []})
    else:
        w_eq = np.repeat(1 / len(active_cols), len(active_cols))
        pos_df = pd.DataFrame({
            "Ativo": active_cols,
            "Peso (%)": (w_eq * 100).round(2)
        })

# Exibição
if show_pie and len(pos_df) > 0:
    fig_pie = px.pie(pos_df, names="Ativo", values="Peso (%)",
                     title="Ponderação das Posições (%)")
    fig_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.dataframe(pos_df, hide_index=True, use_container_width=True)


    # ---- Best / Worst performer (filtrando ativos com peso 0)
    if use_ledger and ledger_ctx is not None:
        # obtém os pesos mais recentes do ledger
        last_w = ledger_ctx["weights"].reindex(rets.index).ffill().iloc[-1]
        ativos_ativos = last_w[last_w.abs() > 1e-6].index.tolist()
    else:
        ativos_ativos = list(rets_active.columns)

    # garante que existam ativos válidos
    if len(ativos_ativos) > 0:
        cum_ret = (1 + rets[ativos_ativos]).cumprod()
        total_ret = cum_ret.iloc[-1] / cum_ret.iloc[0] - 1
        last_10d_ret = cum_ret.iloc[-1] / cum_ret.iloc[max(-10, -len(cum_ret))] - 1
        best = total_ret.idxmax()
        worst = total_ret.idxmin()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Best Performer**")
            if best:
                st.metric(best, f"{total_ret[best]:.2%}", f"10d: {last_10d_ret[best]:.2%}")
            else:
                st.write("—")

        with c2:
            st.markdown("**Worst Performer**")
            if worst:
                st.metric(worst, f"{total_ret[worst]:.2%}", f"10d: {last_10d_ret[worst]:.2%}")
            else:
                st.write("—")
    else:
        st.info("Nenhum ativo com peso diferente de zero no portfólio.")

    # --- Contribuição Mensal de Cada Ativo ---
    if use_ledger and ledger_ctx is not None:
        weights_daily = ledger_ctx["weights"].reindex(rets.index).ffill()
        weights_ex_cash = weights_daily.drop(
            columns=[c for c in weights_daily.columns if c == "CASH"], errors="ignore"
        )
        weights_lag = weights_ex_cash.shift(1).fillna(method="bfill")
        contrib_daily = rets.mul(weights_lag, axis=0)
        contrib_monthly = contrib_daily.resample("M").sum() * 100
        contrib_long = contrib_monthly.reset_index().melt(
            id_vars="date", var_name="Ativo", value_name="Contribuição (%)"
        )
    else:
        rets_monthly = (1 + rets_active).resample("M").prod() - 1
        w_eq = np.repeat(1/len(rets_active.columns), len(rets_active.columns)) if len(rets_active.columns) > 0 else []
        contrib = rets_monthly.mul(w_eq, axis=1) * 100
        contrib_long = contrib.reset_index().melt(
            id_vars="date", var_name="Ativo", value_name="Contribuição (%)"
        )

    fig = px.bar(
        contrib_long, x="date", y="Contribuição (%)",
        color="Ativo", barmode="group",
        title="Contribuição Mensal de Cada Ativo ao Retorno do Portfólio"
    )
    fig.update_layout(xaxis_title="Mês", yaxis_title="Contribuição (%)")
    st.plotly_chart(fig, use_container_width=True)

    # ---- Evolução do Portfólio vs Referências ----
    st.markdown("#### Evolução do Portfólio vs Referências")
    evol = (1 + rets_active).cumprod()
    evol["Portfólio"] = curva_port

    bench_curves = pd.DataFrame(index=evol.index)
    if not bench_px.empty:
        for col in bench_px.columns:
            bench_curves[col] = bench_px[col].reindex(evol.index).ffill().dropna()
        bench_curves = bench_curves.apply(lambda s: s / s.iloc[0], axis=0)
    if not cdi_curve.empty:
        bench_curves["CDI 100%"] = cdi_curve.reindex(evol.index).ffill()

    comp = pd.concat([evol[["Portfólio"]], bench_curves], axis=1).dropna(how="all")
    fig_ev = px.line(comp, title="Evolução (normalizada em 1,00)")
    st.plotly_chart(fig_ev, use_container_width=True)

    # ---- Variação Percentual Diária do Portfólio ----
    st.markdown("#### Variação Percentual Diária do Portfólio")

    # Cria DataFrame com retorno diário e converte em percentual
    daily_var_df = pd.DataFrame({
        "Data": port_daily.index,
        "Retorno Diário (%)": port_daily * 100
    }).reset_index(drop=True)

    # Define cores: azul para dias positivos, vermelho para negativos
    daily_var_df["Cor"] = np.where(daily_var_df["Retorno Diário (%)"] >= 0, "rgba(38,150,228,0.8)", "rgba(228,50,38,0.8)")

    # Gráfico de barras
    fig_bar = px.bar(
        daily_var_df,
        x="Data",
        y="Retorno Diário (%)",
        title="Variação Percentual Diária do Portfólio",
        color="Cor",
        color_discrete_map="identity"
    )

    # Layout limpo e responsivo
    fig_bar.update_layout(
        showlegend=False,
        xaxis_title="Data",
        yaxis_title="Retorno Diário (%)",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True),
        bargap=0.1
    )

    st.plotly_chart(fig_bar, use_container_width=True)

    # ---- Métricas do Período ----
    st.markdown("#### Métricas do Período")
    if use_ledger and ledger_ctx is not None:
        aportes = 0.0
        serie_port = ledger_ctx["port_value"].reindex(rets.index).ffill()
        final_value = float(serie_port.iloc[-1])
        net_profit = final_value - initial_capital - aportes
        twr_total_return = final_value / initial_capital - 1
        years = max((serie_port.index[-1] - serie_port.index[0]).days / 365.25, 1e-9)
        twr_cagr = (1 + twr_total_return) ** (1 / years) - 1
        vol_ann = port_daily.std() * np.sqrt(TRADING_DAYS)
    else:
        trades_df = pd.DataFrame(st.session_state.trades)
        aportes = 0.0
        if not trades_df.empty:
            mask_period = (trades_df["date"] >= start_date) & (trades_df["date"] <= end_date)
            aportes = float((trades_df.loc[mask_period, "qty"] * trades_df.loc[mask_period, "price"]).sum())
        serie_port = curva_port * initial_capital
        final_value = float(serie_port.iloc[-1] + aportes)
        net_profit = final_value - initial_capital - aportes
        twr_total_return = serie_port.iloc[-1] / initial_capital - 1
        years = max((serie_port.index[-1] - serie_port.index[0]).days / 365.25, 1e-9)
        twr_cagr = (1 + twr_total_return) ** (1 / years) - 1
        vol_ann = (rets_active @ np.repeat(1/len(rets_active.columns), len(rets_active.columns))).std() * np.sqrt(TRADING_DAYS)


    m1, m2, m3, m4, m5, m6 = st.columns(6)
    cur_symbol = "$" if use_ledger else "R$"
    fmt_money = f"{cur_symbol} {{:,.0f}}".format(final_value).replace(",", "X").replace(".", ",").replace("X", ".")
    fmt_aport = f"{cur_symbol} {{:,.0f}}".format(aportes).replace(",", "X").replace(".", ",").replace("X", ".")
    fmt_lucro = f"{cur_symbol} {{:,.0f}}".format(net_profit).replace(",", "X").replace(".", ",").replace("X", ".")

    m1.metric("Valor final", fmt_money)
    m2.metric("Aportes", fmt_aport)
    m3.metric("Lucro líquido", fmt_lucro)
    m4.metric("TWR Retorno total", f"{twr_total_return:.2%}")
    m5.metric("TWR CAGR", f"{twr_cagr:.2%}")
    m6.metric("Volatilidade (anual)", f"{vol_ann:.2%}")

# ---- Cálculo e exibição do "Cash" (residual) ----
if use_ledger and ledger_ctx is not None:
    # Últimos pesos conhecidos (já sem CASH)
    last_w = ledger_ctx["weights"].reindex(rets.index).ffill().iloc[-1]
    last_w = last_w[last_w.abs() > 1e-6]
    if "CASH" in last_w.index:
        last_w = last_w.drop("CASH")
    soma_pesos = float(last_w.sum())
else:
    soma_pesos = 1.0  # modo não-ledger já assume pesos normalizados

# Cash = 100% - soma dos pesos ativos
cash_percent = max(0.0, 1.0 - soma_pesos)
cash_value = cash_percent * final_value

st.markdown("#### 💵 Composição de Caixa (CASH)")
c1, c2 = st.columns(2)
c1.metric("Cash (%)", f"{cash_percent*100:.2f}%")
c2.metric("Cash (valor)", f"{cur_symbol}{cash_value:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

# ============= RISCOS =============
with tab_risk:
    st.subheader("Métricas do Portfólio (Monitoramento)")
    
    # beta vs SP500
    bench_ret = None
    if "SP500" in bench_px.columns:
        bench_ret = bench_px["SP500"].pct_change().reindex(rets.index).dropna()
    port_daily_series = port_daily.reindex(rets.index).dropna()

    if bench_ret is not None and len(bench_ret.dropna()) > 10 and len(port_daily_series.dropna()) > 10:
        common_idx = port_daily_series.dropna().index.intersection(bench_ret.dropna().index)
        cov = np.cov(port_daily_series.loc[common_idx], bench_ret.loc[common_idx])[0, 1]
        var = np.var(bench_ret.loc[common_idx])
        beta = cov / var if var > 0 else np.nan

    if use_ledger:
        # Sharpe com base na série do portfólio
        mu_a = port_daily_series.mean() * TRADING_DAYS
        vol_a_r = port_daily_series.std() * np.sqrt(TRADING_DAYS)
        sharpe_r = (mu_a - rf_annual) / vol_a_r if vol_a_r > 0 else np.nan
    else:
        _, vol_a_r, sharpe_r = portfolio_stats(rets, w_real, rf_annual)

    dd_series_r, dd_min_r = drawdown_curve(curva_port)

    try:
        port_std = port_daily.std()  # desvio-padrão dos retornos
        port_mean = port_daily.mean()

        from scipy.stats import norm
        var_95 = -(port_mean + norm.ppf(0.95) * port_std)
        var_99 = -(port_mean + norm.ppf(0.99) * port_std)
    except Exception:
        var_95, var_99 = np.nan, np.nan

    # === Layout das métricas ===
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Beta (SP500)", f"{beta:.2f}" if np.isfinite(beta) else "N/D")
    c2.metric("Sharpe", f"{sharpe_r:.2f}" if np.isfinite(sharpe_r) else "N/D")
    c3.metric("Volatilidade Anualizada", f"{vol_a_r:.2%}" if np.isfinite(vol_a_r) else "N/D")
    c4.metric("Máx. Drawdown", f"{dd_min_r:.2%}")
    c5.metric("VaR 95%", f"{var_95:.2%}" if np.isfinite(var_95) else "N/D")
    c6.metric("VaR 99%", f"{var_99:.2%}" if np.isfinite(var_99) else "N/D")

    st.markdown("#### Volatilidade Móvel – 21 dias (anualizada)")
    # usar a série do portfólio consolidado
    rolling_vol_port = port_daily.rolling(window=21).std() * np.sqrt(TRADING_DAYS)
    rolling_vol_port = rolling_vol_port.dropna().to_frame("Volatilidade Anualizada")

    fig_rv = px.line(
        rolling_vol_port,
        y="Volatilidade Anualizada",
        title="Volatilidade Móvel – 21 dias (anualizada)",
        labels={"index": "Data"}
    )
    fig_rv.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_rv, use_container_width=True)

    st.markdown("#### Drawdown do Portfólio")
    fig_dd = px.area(dd_series_r, title="Drawdown", labels={"value": "drawdown"})
    fig_dd.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig_dd, use_container_width=True)

    st.markdown("#### Volatilidade Anualizada (ativos)")
    vol_tbl = (rets_active.std() * np.sqrt(TRADING_DAYS) * 100).round(2)
    st.dataframe(vol_tbl.rename("Volatilidade (%)"), use_container_width=True)

    st.markdown("#### Heatmap de Correlação (ativos)")
    corr = rets_active.corr().round(2)
    fig_corr = px.imshow(corr, text_auto=True, color_continuous_scale="Blues", title="Correlação dos Retornos Diários")
    st.plotly_chart(fig_corr, use_container_width=True)

# --- Bloco de exportação de gráficos e tabelas principais ---
st.markdown("---")
st.markdown("### 📥 Exportar gráficos e tabelas principais")

exports_dict = {}

try:
    # 📈 Aba RESUMO
    exports_dict["Posicoes_Portfolio"] = pos_df
    exports_dict["Contribuicao_Mensal"] = contrib_long
    exports_dict["Evolucao_Portfolio_vs_Referencias"] = comp
    exports_dict["Variação_Diária_Portfolio"] = daily_var_df  # 🔹 Novo gráfico de variação diária

    # ⚠️ Aba RISCOS
    exports_dict["Volatilidade_Movel_7d"] = rolling_vol_port
    exports_dict["Drawdown_Portfolio"] = dd_series_r.to_frame("Drawdown")
    exports_dict["Volatilidade_Ativos"] = vol_tbl.to_frame("Volatilidade (%)")

except Exception as e:
    st.warning(f"Erro ao preparar dados para exportação: {e}")

if st.button("Gerar arquivo .xlsx"):
    if exports_dict:
        excel_bytes = export_to_excel(exports_dict)
        st.download_button(
            label="Baixar gráficos (.xlsx)",
            data=excel_bytes,
            file_name=f"portfolio_export_{dt.date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning("Nenhum gráfico/tabela disponível para exportar.")

# === Função revisada de otimização ===

def optimize_portfolio(rets: pd.DataFrame, allow_short: bool = False, rf: float = 0.0):

# Calcula a carteira de Máx. Sharpe, garantindo que ativos com peso 0 sejam excluídos das contas a soma dos módulos dos pesos seja igual a 1 (com ou sem short)

# Remove colunas com todos os retornos zerados
    rets = rets.loc[:, rets.std() > 0]
    if rets.empty:
        return pd.Series(dtype=float)


    mean_ret = rets.mean() * 252
    cov_ret = rets.cov() * 252
    n = len(mean_ret)


# Função objetivo: minimizar -Sharpe
    def neg_sharpe(w):
        port_ret = np.dot(w, mean_ret)
        port_vol = np.sqrt(np.dot(w.T, np.dot(cov_ret, w)))
        if port_vol == 0:
            return 1e6
        sharpe = (port_ret - rf) / port_vol
        return -sharpe


# === Restrições ===
    cons = []
    if allow_short:
        # soma dos módulos = 1 (normaliza posições absolutas)
        cons.append({"type": "eq", "fun": lambda w: np.sum(np.abs(w)) - 1})
        bounds = tuple((-1, 1) for _ in range(n))
    else:
        # sem short: pesos positivos e soma = 1
        cons.append({"type": "eq", "fun": lambda w: np.sum(w) - 1})
        bounds = tuple((0, 1) for _ in range(n))


    w0 = np.repeat(1 / n, n)


    res = minimize(neg_sharpe, w0, bounds=bounds, constraints=cons, method="SLSQP")


    if not res.success:
        st.warning(f"Otimização não convergiu: {res.message}")
        return pd.Series(0, index=mean_ret.index)


    weights = pd.Series(res.x, index=mean_ret.index)

    # Zera ativos com peso insignificante (<0.001)
    weights[np.abs(weights) < 0.001] = 0
    weights = weights[weights != 0]

    # Re-normaliza para garantir soma do módulo = 1
    weights = weights / np.sum(np.abs(weights))

    return weights

# ============= OTIMIZAÇÃO =============
with tab_otimiz:
    st.subheader("Fronteira Eficiente e Carteiras Ótimas")

    # Usa apenas os ativos com peso ≠ 0
    if rets_active.shape[1] == 0:
        st.warning("Não há ativos ativos (peso > 0) para otimizar.")
        st.stop()

    mu = rets_active.mean() * TRADING_DAYS
    cov = rets_active.cov() * TRADING_DAYS
    tickers_opt = list(rets_active.columns)
    n = len(tickers_opt)

    # ===== Monte Carlo (∑|w|=1 se short) =====
    rng = np.random.default_rng(42)
    vols, rets_mc = [], []
    sims = int(n_sims)

    for _ in range(sims):
        if allow_short:
            w = rng.normal(0, 1, n)
            if np.sum(np.abs(w)) == 0:
                continue
            w /= np.sum(np.abs(w))
        else:
            w = rng.random(n)
            if w.sum() == 0:
                continue
            w /= w.sum()

        r = float(np.dot(w, mu))
        v = float(np.sqrt(np.dot(w.T, cov @ w)))
        rets_mc.append(r)
        vols.append(v)

    mc_df = pd.DataFrame({"Vol": vols, "Ret": rets_mc})

    # ===== Funções auxiliares =====
    def port_ret(w): return float(np.dot(w, mu))
    def port_vol(w): return float(np.sqrt(np.dot(w.T, cov @ w)))
    def neg_sharpe(w):
        v = port_vol(w)
        return - (port_ret(w) - rf_annual) / v if v > 0 else 1e6

    # ===== Restrições =====
    constraints = []
    if allow_short:
        constraints.append({"type": "eq", "fun": lambda w: np.sum(np.abs(w)) - 1})
        bounds = [(-1.0, 1.0)] * n
        x0 = rng.normal(0, 1, n)
        x0 /= np.sum(np.abs(x0))
    else:
        constraints.append({"type": "eq", "fun": lambda w: np.sum(w) - 1})
        bounds = [(0.0, 1.0)] * n
        x0 = np.repeat(1/n, n)

    # ===== Carteira Máx. Sharpe =====
    res_sharpe = minimize(neg_sharpe, x0=x0, method="SLSQP",
                          bounds=bounds, constraints=constraints)
    if res_sharpe.success:
        w_sharpe = res_sharpe.x
        w_sharpe[np.abs(w_sharpe) < 1e-4] = 0
        w_sharpe /= np.sum(np.abs(w_sharpe)) if allow_short else w_sharpe.sum()
    else:
        st.warning(f"Otimização Máx. Sharpe não convergiu: {res_sharpe.message}")
        w_sharpe = x0

    r_sharpe, v_sharpe = port_ret(w_sharpe), port_vol(w_sharpe)
    s_sharpe = (r_sharpe - rf_annual) / v_sharpe if v_sharpe > 0 else np.nan

    # ===== Carteira Mín. Variância =====
    res_minvar = minimize(port_vol, x0=x0, method="SLSQP",
                          bounds=bounds, constraints=constraints)
    if res_minvar.success:
        w_minvar = res_minvar.x
        w_minvar[np.abs(w_minvar) < 1e-4] = 0
        w_minvar /= np.sum(np.abs(w_minvar)) if allow_short else w_minvar.sum()
    else:
        st.warning(f"Otimização Mín. Variância não convergiu: {res_minvar.message}")
        w_minvar = x0

    r_minvar, v_minvar = port_ret(w_minvar), port_vol(w_minvar)
    s_minvar = (r_minvar - rf_annual) / v_minvar if v_minvar > 0 else np.nan

    # ===== Fronteira Eficiente =====
    r_targets = np.linspace(mu.min(), mu.max()*1.5, 60)
    ef_vols, ef_rets = [], []
    for rt in r_targets:
        cons_rt = constraints + [{"type": "eq", "fun": lambda w, rt=rt: np.dot(w, mu) - rt}]
        res = minimize(lambda w: np.sqrt(np.dot(w.T, cov @ w)),
                       x0=x0, method="SLSQP", bounds=bounds, constraints=cons_rt)
        if res.success:
            ef_vols.append(np.sqrt(np.dot(res.x.T, cov @ res.x)))
            ef_rets.append(np.dot(res.x, mu))

    # ===== Plot =====
    fig = go.Figure()
    if len(mc_df):
        fig.add_trace(go.Scatter(x=mc_df["Vol"], y=mc_df["Ret"], mode="markers",
                                 name="Portfólios Aleatórios", marker=dict(size=4, opacity=0.45)))
    if len(ef_vols):
        fig.add_trace(go.Scatter(x=ef_vols, y=ef_rets, mode="lines",
                                 name="Fronteira Eficiente", line=dict(width=3)))
    fig.add_trace(go.Scatter(x=[v_sharpe], y=[r_sharpe], mode="markers",
                             name="Máx. Sharpe", marker=dict(size=10, symbol="star")))
    fig.add_trace(go.Scatter(x=[v_minvar], y=[r_minvar], mode="markers",
                             name="Mín. Variância", marker=dict(size=10)))
    fig.update_layout(xaxis_title="Volatilidade (anual)",
                      yaxis_title="Retorno (anual)",
                      title="Fronteira Eficiente + Monte Carlo")
    st.plotly_chart(fig, use_container_width=True)

    # ===== Tabelas =====
    st.markdown("### 🚀 Carteira Ótima (Máx. Sharpe)")
    df_sharpe = pd.DataFrame({"Ticker": tickers_opt, "Peso (%)": (w_sharpe*100).round(2)})
    df_sharpe = df_sharpe.loc[df_sharpe["Peso (%)"].abs() > 0.01]
    st.dataframe(df_sharpe, hide_index=True)
    st.write(f"Retorno: {r_sharpe:.2%}, Volatilidade: {v_sharpe:.2%}, Sharpe: {s_sharpe:.2f}")

    st.markdown("### 🌟 Carteira Mín. Variância")
    df_minvar = pd.DataFrame({"Ticker": tickers_opt, "Peso (%)": (w_minvar*100).round(2)})
    df_minvar = df_minvar.loc[df_minvar["Peso (%)"].abs() > 0.01]
    st.dataframe(df_minvar, hide_index=True)
    st.write(f"Retorno: {r_minvar:.2%}, Volatilidade: {v_minvar:.2%}, Sharpe: {s_minvar:.2f}")


# ============= FORECAST =============
with tab_forecast:
    st.subheader("Simulação de Monte Carlo – Forecast do Portfólio")

    horizon_days = st.number_input("Horizonte (dias úteis)", min_value=20, max_value=252*5, value=252)
    mc_paths = st.number_input("Nº de trajetórias", min_value=100, max_value=50000, value=5000, step=500)

    port_ret_daily = port_daily.dropna()
    mu_d = port_ret_daily.mean()
    sigma_d = port_ret_daily.std()

    start_value = st.number_input("Valor inicial simulado ($)", min_value=1000.0, value=float(initial_capital), step=1000.0, format="%.2f") if use_ledger else \
                  st.number_input("Valor inicial simulado (R$)", min_value=1000.0, value=100000.0, step=1000.0, format="%.2f")

    rng = np.random.default_rng(123)
    # Geometric Brownian Motion ~ lognormal
    shocks = rng.normal((mu_d - 0.5*sigma_d**2), sigma_d, size=(int(mc_paths), int(horizon_days)))
    paths = float(start_value) * np.exp(np.cumsum(shocks, axis=1))
    # série percentis
    pct = np.percentile(paths, [5, 25, 50, 75, 95], axis=0)
    idx = pd.bdate_range(end=end_date, periods=int(horizon_days))
    df_pct = pd.DataFrame(pct.T, index=idx, columns=["p5", "p25", "p50", "p75", "p95"])

    fig_fc = go.Figure()
    fig_fc.add_trace(go.Scatter(x=df_pct.index, y=df_pct["p50"], name="Mediana"))
    fig_fc.add_trace(go.Scatter(x=df_pct.index, y=df_pct["p95"], name="p95", line=dict(width=1)))
    fig_fc.add_trace(go.Scatter(x=df_pct.index, y=df_pct["p5"], name="p5", line=dict(width=1), fill='tonexty',
                                fillcolor="rgba(0,0,255,0.08)"))
    fig_fc.add_trace(go.Scatter(x=df_pct.index, y=df_pct["p75"], name="p75", line=dict(width=1)))
    fig_fc.add_trace(go.Scatter(x=df_pct.index, y=df_pct["p25"], name="p25", line=dict(width=1), fill='tonexty',
                                fillcolor="rgba(0,0,255,0.12)"))
    fig_fc.update_layout(title="Faixas de Cenário (p5/p25/mediana/p75/p95)")
    st.plotly_chart(fig_fc, use_container_width=True)

    # distribuição de valores finais
    final_vals = paths[:, -1]
    fig_hist = px.histogram(final_vals, nbins=50, title="Distribuição do Valor Final")
    st.plotly_chart(fig_hist, use_container_width=True)

# ============= CHAT (contexto completo, mas oculto na UI) =============
with tab_chat:
    st.subheader("Converse com a IA (Gemini) sobre o portfólio e o mercado")

    # tabelas de estatísticas
    cov_table   = rets.cov().round(4).to_markdown()
    stats_table = (pd.DataFrame({
        "Retorno (%)": (rets.mean() * TRADING_DAYS * 100).round(2),
        "Volatilidade (%)": (rets.std() * np.sqrt(TRADING_DAYS) * 100).round(2)
    })).to_markdown()

    if not use_ledger:
        sharpe_info = (
            f"Sharpe (pesos fixos) = {(portfolio_stats(rets, w_real, rf_annual)[2]):.2f}, "
            f"Pesos={dict(zip(tickers, (np.array(w_real)*100).round(2)))}"
        )
    else:
        mu_a = port_daily.mean() * TRADING_DAYS
        vol_a = port_daily.std() * np.sqrt(TRADING_DAYS)
        sharpe_val = (mu_a - rf_annual) / vol_a if vol_a > 0 else np.nan
        last_w = ledger_ctx["weights"].reindex(rets.index).ffill().iloc[-1]
        sharpe_info = (
            f"Sharpe (ledger) = {sharpe_val:.2f}, "
            f"Pesos atuais = {dict(zip(last_w.index, (last_w.values*100).round(2)))}"
        )

    # métricas adicionais
    dd_series_r, dd_min = drawdown_curve(curva_port)
    ctx_metrics = {
        "Valor final": float((ledger_ctx["port_value"].iloc[-1]) if (use_ledger and ledger_ctx is not None) else (curva_port.iloc[-1]*initial_capital)),
        "TWR total": float((ledger_ctx["port_value"].iloc[-1]/ledger_ctx["port_value"].iloc[0]-1) if use_ledger else (curva_port.iloc[-1]-1)),
        "Vol (anual)": float(port_daily.std()*np.sqrt(TRADING_DAYS)),
        "Max DD": float(dd_min)
    }
    ctx_metrics_str = "\n".join([f"- {k}: {v}" for k, v in ctx_metrics.items()])

    # >>> Retornos diários e semanais para contexto
    daily_port   = port_daily
    weekly_assets = (1 + rets).resample("W").prod() - 1
    daily_table_chat  = daily_port.round(4).to_frame("Retorno Diário").to_markdown()
    weekly_table_chat = weekly_assets.round(4).to_markdown()

    # contexto consolidado para o Gemini
    ctx = (
        f"Período analisado: {start_date} → {end_date}\n"
        f"Taxa livre de risco: {rf_annual:.2%}\n\n"
        f"Estatísticas dos ativos:\n{stats_table}\n\n"
        f"Matriz de Covariância anual:\n{cov_table}\n\n"
        f"{sharpe_info}\n\n"
        f"Métricas adicionais:\n{ctx_metrics_str}\n\n"
        f"Retorno diário consolidado do portfólio:\n{daily_table_chat}\n\n"
        f"Retorno semanal por ativo:\n{weekly_table_chat}\n"
    )

    st.caption("O chat usa todas as métricas e tabelas da aplicação como contexto (não exibidas).")

    # inicia sessão de chat
    if "chat" not in st.session_state and gemini_model:
        st.session_state.chat = gemini_model.start_chat(history=[])

    prompt = st.chat_input("Digite sua pergunta")
    if prompt and gemini_model:
        with st.chat_message("user"):
            st.write(prompt)
        with st.chat_message("assistant"):
            try:
                resp = st.session_state.chat.send_message(
                    SYSTEM_INSTRUCTIONS
                    + f"Use o seguinte contexto para responder matematicamente, com fórmulas quando fizer sentido:\n"
                    + ctx
                    + f"\n\nPergunta: {prompt}"
                )
                st.write(resp.text)
            except Exception as e:
                st.error(f"Falha ao consultar Gemini: {e}")
    elif prompt and not gemini_model:
        st.warning("Configure GOOGLE_API_KEY para usar o chat Gemini.")