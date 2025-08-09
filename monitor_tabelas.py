from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests


DOWNLOAD_URL = "https://www.concity.com.br/arquivos/599da4243044a07f6b3a9986d46c35b2.csv"
ARQUIVOS_ORIGINAIS = [
    "TabelaIBPTaxBA15.1.B.csv",
    "TabelaIBPTax15.1.B.csv",
]


def ler_csv(caminho_arquivo: Path) -> pd.DataFrame:
    tentativas = [
        {},
        {"encoding": "utf-8"},
        {"encoding": "latin-1"},
    ]
    ultimo_erro: Optional[Exception] = None
    for kwargs in tentativas:
        try:
            return pd.read_csv(caminho_arquivo, sep=None, engine="python", **kwargs)
        except Exception as exc:  # noqa: BLE001 - tratamos genericamente para re-tentar com outro encoding
            ultimo_erro = exc
    if ultimo_erro:
        raise ultimo_erro
    raise RuntimeError("Falha inesperada ao ler o CSV.")


def verificar_validade(caminho_arquivo: str | Path) -> Tuple[bool, date]:
    caminho = Path(caminho_arquivo)
    try:
        df = ler_csv(caminho)
    except FileNotFoundError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Erro ao ler '{caminho.name}': {exc}") from exc

    if "vigenciafim" not in df.columns:
        raise ValueError(
            f"Coluna 'vigenciafim' não encontrada em '{caminho.name}'. Colunas: {list(df.columns)}"
        )

    try:
        datas = pd.to_datetime(df["vigenciafim"], dayfirst=True, errors="coerce").dt.date
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Falha convertendo 'vigenciafim' em '{caminho.name}': {exc}") from exc

    datas_validas = [d for d in datas.dropna().tolist() if isinstance(d, date)]
    if not datas_validas:
        raise ValueError(f"Nenhuma data válida encontrada em 'vigenciafim' de '{caminho.name}'.")

    ultima_data = max(datas_validas)
    hoje = date.today()
    expirado = ultima_data < hoje
    return expirado, ultima_data


def renomear_arquivo_antigo(caminho_arquivo: str | Path, data_vencimento: date) -> Path:
    caminho = Path(caminho_arquivo)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    data_fmt = data_vencimento.strftime("%d-%m-%Y")
    novo_nome_base = f"{caminho.stem}_{data_fmt}{caminho.suffix}"
    novo_caminho = caminho.with_name(novo_nome_base)

    if novo_caminho.exists():
        contador = 1
        while True:
            candidato = caminho.with_name(f"{caminho.stem}_{data_fmt}({contador}){caminho.suffix}")
            if not candidato.exists():
                novo_caminho = candidato
                break
            contador += 1

    caminho.rename(novo_caminho)
    return novo_caminho


def baixar_nova_tabela(url: str, nome_arquivo: str | Path) -> Path:
    destino = Path(nome_arquivo)
    temporario = destino.with_suffix(destino.suffix + ".tmp")

    try:
        resp = requests.get(url, timeout=60, stream=True, headers={"User-Agent": "monitor-tabelas/1.0"})
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Falha no download: {exc}") from exc

    try:
        with open(temporario, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        # Verifica tamanho > 0
        if temporario.stat().st_size == 0:
            raise RuntimeError("Arquivo baixado está vazio.")
        temporario.replace(destino)
    except Exception as exc:  # noqa: BLE001
        if temporario.exists():
            try:
                temporario.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass
        raise RuntimeError(f"Erro ao salvar '{destino.name}': {exc}") from exc

    return destino


def processar_arquivo(caminho_arquivo: str, url_download: str) -> bool:
    caminho = Path(caminho_arquivo)
    try:
        if not caminho.exists():
            print(f"[AVISO] '{caminho.name}' não encontrado. Baixando novo arquivo...")
            baixar_nova_tabela(url_download, caminho)
            print(f"[OK] Novo arquivo salvo: {caminho.name}")
            return True

        expirado, data_venc = verificar_validade(caminho)
        if expirado:
            novo_nome = renomear_arquivo_antigo(caminho, data_venc)
            print(f"[INFO] Arquivo vencido em {data_venc.strftime('%d-%m-%Y')}. Renomeado para '{novo_nome.name}'.")
            baixar_nova_tabela(url_download, caminho)
            print(f"[OK] Novo arquivo salvo: {caminho.name}")
        else:
            print(
                f"[OK] '{caminho.name}' ainda está válido. Vencimento: {data_venc.strftime('%d-%m-%Y')}."
            )
        return True
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"[ERRO] {exc}")
        return False


def main() -> int:
    houve_falha = False
    for nome in ARQUIVOS_ORIGINAIS:
        sucesso = processar_arquivo(nome, DOWNLOAD_URL)
        if not sucesso:
            houve_falha = True
    return 1 if houve_falha else 0


if __name__ == "__main__":
    sys.exit(main())


