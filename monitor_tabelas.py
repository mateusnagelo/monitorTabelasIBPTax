from __future__ import annotations

import sys
import argparse
import os
import ctypes
import logging
from logging.handlers import RotatingFileHandler
from time import sleep
from datetime import date
from pathlib import Path
from typing import Optional, Tuple
from threading import Event, Thread
import subprocess
import winreg

import pandas as pd
import requests


DOWNLOAD_URL = "https://www.concity.com.br/arquivos/599da4243044a07f6b3a9986d46c35b2.csv"
ARQUIVOS_ORIGINAIS = [
    "TabelaIBPTaxBA15.1.B.csv",
    "TabelaIBPTax15.1.B.csv",
]

# Metadados do aplicativo
APP_NAME = "MonitorTabelaIBPTax"
APP_VERSION = "1.0.2"
APP_DEVELOPER = "Mateus Angelo"
# Texto de exibição (bandeja/notificações)
APP_DISPLAY = f"Monitor de Tabelas IBPTax v{APP_VERSION}"


def _get_startup_key():
    """Retorna a chave de registro do startup do Windows."""
    try:
        return winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            0,
            winreg.KEY_READ | winreg.KEY_WRITE
        )
    except Exception:
        return None


def _is_in_startup() -> bool:
    """Verifica se o programa está no inicializador do Windows."""
    key = _get_startup_key()
    if not key:
        return False
    try:
        winreg.QueryValueEx(key, APP_NAME)
        return True
    except FileNotFoundError:
        return False
    finally:
        try:
            winreg.CloseKey(key)
        except Exception:
            pass


def _add_to_startup() -> bool:
    """Adiciona o programa ao inicializador do Windows."""
    key = _get_startup_key()
    if not key:
        return False
    try:
        exe_path = str(Path(sys.executable).resolve())
        winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ, f'"{exe_path}"')
        return True
    except Exception:
        return False
    finally:
        try:
            winreg.CloseKey(key)
        except Exception:
            pass


def _remove_from_startup() -> bool:
    """Remove o programa do inicializador do Windows."""
    key = _get_startup_key()
    if not key:
        return False
    try:
        winreg.DeleteValue(key, APP_NAME)
        return True
    except FileNotFoundError:
        return True  # Já não estava lá
    except Exception:
        return False
    finally:
        try:
            winreg.CloseKey(key)
        except Exception:
            pass


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
            logging.info("Arquivo '%s' não encontrado. Baixando novo arquivo...", caminho.name)
            baixar_nova_tabela(url_download, caminho)
            print(f"[OK] Novo arquivo salvo: {caminho.name}")
            logging.info("Novo arquivo salvo: %s", caminho.name)
            return True

        expirado, data_venc = verificar_validade(caminho)
        if expirado:
            novo_nome = renomear_arquivo_antigo(caminho, data_venc)
            print(f"[INFO] Arquivo vencido em {data_venc.strftime('%d-%m-%Y')}. Renomeado para '{novo_nome.name}'.")
            logging.info(
                "Arquivo '%s' vencido em %s. Renomeado para '%s'",
                caminho.name,
                data_venc.strftime('%d-%m-%Y'),
                novo_nome.name,
            )
            baixar_nova_tabela(url_download, caminho)
            print(f"[OK] Novo arquivo salvo: {caminho.name}")
            logging.info("Novo arquivo salvo: %s", caminho.name)
        else:
            print(
                f"[OK] '{caminho.name}' ainda está válido. Vencimento: {data_venc.strftime('%d-%m-%Y')}."
            )
            logging.info("Arquivo '%s' ainda válido. Vencimento: %s", caminho.name, data_venc.strftime('%d-%m-%Y'))
        return True
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"[ERRO] {exc}")
        logging.error("Erro processando '%s': %s", caminho.name, exc)
        return False


def _resolve_base_dir() -> Path:
    if getattr(sys, "frozen", False):  # executável PyInstaller
        return Path(sys.executable).parent
    return Path(__file__).parent


def _setup_logging(base_dir: Path) -> None:
    log_path = base_dir / f"{APP_NAME}.log"
    handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[handler],
        force=True,
    )
    logging.info("%s v%s por %s", APP_NAME, APP_VERSION, APP_DEVELOPER)
    logging.info("Log iniciado em %s", log_path)


def _acquire_lock(base_dir: Path) -> Path:
    lock_path = base_dir / f"{APP_NAME}.lock"
    # Implementação simples: criar com 'x' para evitar múltiplas instâncias
    try:
        with open(lock_path, "x", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return lock_path
    except FileExistsError:
        raise RuntimeError(
            f"Outra instância já está em execução. Se for engano, apague o arquivo '{lock_path.name}' e tente novamente."
        )


def _release_lock(lock_path: Optional[Path]) -> None:
    if lock_path and lock_path.exists():
        try:
            lock_path.unlink()
        except Exception:
            pass


def executar_uma_vez(base_dir: Path) -> int:
    houve_falha = False
    for nome in ARQUIVOS_ORIGINAIS:
        arquivo = base_dir / nome
        sucesso = processar_arquivo(str(arquivo), DOWNLOAD_URL)
        if not sucesso:
            houve_falha = True
    return 1 if houve_falha else 0


def executar_em_background(base_dir: Path, intervalo_minutos: int, stop_event: Optional[Event] = None) -> int:
    lock_path: Optional[Path] = None
    try:
        lock_path = _acquire_lock(base_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERRO] {exc}")
        logging.error("%s", exc)
        return 1

    logging.info("%s iniciado em background. Intervalo: %d minutos", APP_NAME, intervalo_minutos)
    print(f"[INFO] {APP_NAME} iniciado em background. Intervalo: {intervalo_minutos} minuto(s). Logs em '{APP_NAME}.log'.")
    try:
        if stop_event is None:
            stop_event = Event()
        while not stop_event.is_set():
            exit_code = executar_uma_vez(base_dir)
            logging.info("Ciclo concluído com status: %s", "ERRO" if exit_code else "OK")
            # aguarda próximo ciclo com possibilidade de cancelamento
            total_segundos = max(1, int(intervalo_minutos * 60))
            for _ in range(total_segundos):
                if stop_event.wait(1):
                    break
    except KeyboardInterrupt:
        logging.info("Encerrado por solicitação do usuário.")
        return 0
    finally:
        _release_lock(lock_path)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor de validade das tabelas IBPTax.")
    parser.add_argument("--once", action="store_true", help="Executa apenas uma vez e encerra.")
    parser.add_argument("--headless", action="store_true", help="Executa em background sem ícone na bandeja.")
    parser.add_argument(
        "--interval",
        type=int,
        default=360,
        help="Intervalo em minutos entre verificações no modo background (padrão: 360).",
    )
    return parser.parse_args(argv)


def _is_already_running(base_dir: Path) -> bool:
    antigo = base_dir / "monitor_tabelas.lock"
    novo = base_dir / f"{APP_NAME}.lock"
    return antigo.exists() or novo.exists()


def _message_box_info(title: str, message: str) -> None:
    try:
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x40)
    except Exception:
        # Fallback silencioso em ambientes não-Windows
        print(f"{title}: {message}")


def _generate_tray_icon_image() -> "Image.Image":
    # Criar ícone simples em memória para bandeja
    from PIL import Image, ImageDraw

    size = 64
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    # Fundo azul
    draw.ellipse((4, 4, size - 4, size - 4), fill=(0, 102, 204, 255))
    # Símbolo de check branco
    draw.line((18, 34, 28, 44), fill=(255, 255, 255, 255), width=6)
    draw.line((28, 44, 46, 22), fill=(255, 255, 255, 255), width=6)
    return img


def executar_com_tray(base_dir: Path, intervalo_minutos: int) -> int:
    # Executa o monitor em thread + ícone na bandeja com menu para controle
    try:
        import pystray
        from pystray import Menu, MenuItem
    except Exception as exc:  # noqa: BLE001
        logging.error("Falha ao iniciar ícone de bandeja: %s", exc)
        # fallback: headless
        return executar_em_background(base_dir, intervalo_minutos)

    stop_event: Event = Event()

    def acao_verificar_agora(icon, item):  # noqa: ARG001
        Thread(target=executar_uma_vez, args=(base_dir,), daemon=True).start()

    def acao_abrir_pasta(icon, item):  # noqa: ARG001
        try:
            os.startfile(str(base_dir))  # type: ignore[attr-defined]
        except Exception:
            subprocess.Popen(["explorer", str(base_dir)])

    def acao_abrir_log(icon, item):  # noqa: ARG001
        log_path = base_dir / f"{APP_NAME}.log"
        try:
            os.startfile(str(log_path))  # type: ignore[attr-defined]
        except Exception:
            subprocess.Popen(["notepad", str(log_path)])

    def acao_startup(icon, item):  # noqa: ARG001
        if _is_in_startup():
            if _remove_from_startup():
                try:
                    icon.notify("Removido do inicializador do Windows.", APP_DISPLAY)
                except Exception:
                    _message_box_info(APP_DISPLAY, "Removido do inicializador do Windows.")
            else:
                try:
                    icon.notify("Erro ao remover do inicializador.", APP_DISPLAY)
                except Exception:
                    _message_box_info(APP_DISPLAY, "Erro ao remover do inicializador.")
        else:
            if _add_to_startup():
                try:
                    icon.notify("Adicionado ao inicializador do Windows.", APP_DISPLAY)
                except Exception:
                    _message_box_info(APP_DISPLAY, "Adicionado ao inicializador do Windows.")
            else:
                try:
                    icon.notify("Erro ao adicionar ao inicializador.", APP_DISPLAY)
                except Exception:
                    _message_box_info(APP_DISPLAY, "Erro ao adicionar ao inicializador.")

    def acao_sair(icon, item):  # noqa: ARG001
        stop_event.set()
        try:
            icon.visible = False
            icon.stop()
        except Exception:
            pass

    # Thread de monitoramento
    worker = Thread(target=executar_em_background, args=(base_dir, intervalo_minutos, stop_event), daemon=True)
    worker.start()

    # Criar ícone e menu
    image = _generate_tray_icon_image()
    startup_text = "Remover do inicializador" if _is_in_startup() else "Adicionar ao inicializador"
    menu = Menu(
        MenuItem("Verificar agora", acao_verificar_agora),
        MenuItem("Abrir pasta", acao_abrir_pasta),
        MenuItem("Abrir log", acao_abrir_log),
        MenuItem(startup_text, acao_startup),
        MenuItem("Sair", acao_sair),
    )
    icon = pystray.Icon(name=APP_NAME.lower(), title=APP_DISPLAY, icon=image, menu=menu)

    try:
        def on_ready(icon_: "pystray.Icon") -> None:
            try:
                icon_.visible = True
            except Exception:
                pass
            # Adiciona automaticamente ao inicializador na primeira execução
            if not _is_in_startup():
                _add_to_startup()
                logging.info("Adicionado automaticamente ao inicializador do Windows.")
            # Notifica que iniciou com sucesso, com fallbacks
            notified = False
            try:
                icon_.notify("Monitor iniciado com sucesso.", APP_DISPLAY)
                notified = True
            except Exception:
                notified = False
            if not notified:
                try:
                    from win10toast import ToastNotifier

                    toaster = ToastNotifier()
                    toaster.show_toast(
                        APP_DISPLAY,
                        "Monitor iniciado com sucesso.",
                        duration=5,
                        threaded=True,
                    )
                    notified = True
                except Exception:
                    notified = False
            if not notified:
                _message_box_info(APP_DISPLAY, "Monitor iniciado com sucesso.")

        icon.run(setup=on_ready)
    finally:
        stop_event.set()
        worker.join(timeout=5)
    return 0


def main() -> int:
    base_dir = _resolve_base_dir()
    _setup_logging(base_dir)

    args = parse_args(sys.argv[1:])
    if args.once:
        return executar_uma_vez(base_dir)
    # Se já estiver rodando, avisa e sai
    if _is_already_running(base_dir):
        _message_box_info(APP_NAME, "O monitor já está em execução.")
        print("[INFO] O monitor já está em execução.")
        logging.info("O monitor já está em execução. Encerrando nova instância.")
        return 0
    if args.headless:
        return executar_em_background(base_dir, args.interval)
    return executar_com_tray(base_dir, args.interval)


if __name__ == "__main__":
    sys.exit(main())


