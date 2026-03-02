import yfinance as yf
import pandas as pd

STOCKS = ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "BRK-B", "LLY", "TSLA", "AVGO"]

def run_collector():
    # Descarga el último año de datos
    df = yf.download(STOCKS, period="1y", interval="1d")

    # Limpieza y validación de consistencia
    data = df['Adj Close'].dropna(how='all')
    data.index = pd.to_datetime(data.index)

    # Guardar en formato Parquet (Eficiencia máxima)
    data.to_parquet("market_data.parquet", engine='pyarrow', compression='snappy')
    print("Dataset actualizado exitosamente.")

if __name__ == "__main__":
    run_collector()
