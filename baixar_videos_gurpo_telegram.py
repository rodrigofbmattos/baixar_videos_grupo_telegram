"""
baixar_videos_grupo_telegram.py
-------------------------------
Aplicação que baixa vídeos de um canal do Telegram, organizando os vídeos automaticamente em pastas conforme a hierarquia textual da mensagem.

Requisitos
----------
    pip install telethon
    pip install tqdm
    pip install python-dateutil
    pip install aiofiles
"""

import os
import re
import json
import hashlib
import asyncio
from datetime import timezone
from dateutil import parser as dateparser
from telethon import TelegramClient
from telethon.tl.types import Message, DocumentAttributeVideo, MessageMediaDocument
from telethon.errors import FloodWaitError
from tqdm.asyncio import tqdm
import aiofiles

# -------------------------
# ⚙️ CONFIGURAÇÕES GERAIS
# -------------------------
API_ID = 24033066  # Substitua pelo seu API ID (int)
API_HASH = "3b164c3f1556af9f18e88abc7d10a71e"  # Substitua pelo seu API HASH
NOME_SESSAO = "sessao_telegram"  # Nome do arquivo de sessão (para login persistente)

# ID ou link do canal/grupo
CHAT_ORIGEM = -1002173671489

# Pasta raiz onde os vídeos serão organizados
PASTA_DESTINO = r"D:\Asimov Academy - 2024\Todas as Trilhas - Asimov Academy - 2024"

# -------------------------
# 🧮 REGRAS DE FILTRAGEM
# -------------------------
SOMENTE_VIDEOS = True
TAMANHO_MINIMO = 0
TAMANHO_MAXIMO = 0
DATA_INICIAL = None
DATA_FINAL = None
LIMITE_ARQUIVOS = 0
IGNORAR_REENVIOS = False
IGNORAR_RESPOSTAS = False
EVITAR_DUPLICADOS = True
DOWNLOADS_CONCORRENTES = 3
ARQUIVO_METADADOS = "metadados.json"
MAXIMO_TENTATIVAS = 6

# Variável global para armazenar o total de arquivos baixados
contador_baixados = 0 

# -------------------------
# 🔧 FUNÇÕES AUXILIARES
# -------------------------
def criar_pasta(caminho):
    """
    Cria pasta de forma segura (não dá erro se já existir).
    """
    os.makedirs(caminho, exist_ok=True)

def parsear_data(valor):
    """
    Converte uma string de data para datetime UTC.
    """
    if not valor:
        return None
    return dateparser.parse(valor).astimezone(timezone.utc)

def calcular_sha1(arquivo):
    """
    Calcula o hash SHA1 de um arquivo local.
    """
    h = hashlib.sha1()
    with open(arquivo, "rb") as f:
        for bloco in iter(lambda: f.read(8192), b""):
            h.update(bloco)
    return h.hexdigest()

def nome_arquivo_seguro(nome):
    """
    Remove caracteres inválidos do nome do arquivo.
    """
    invalidos = '<>:"/\\|?*\n\r\t'
    return "".join(c if c not in invalidos else "_" for c in nome)

def carregar_metadados(caminho_metadados):
    """
    Carrega o arquivo JSON de metadados existentes.
    """
    if os.path.exists(caminho_metadados):
        try:
            with open(caminho_metadados, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Erro ao ler metadados (JSON): {e}")
            return []
    return []

# -----------------------------------------------------
# 📁 GERAÇÃO DA HIERARQUIA DE PASTAS A PARTIR DO TEXTO
# -----------------------------------------------------
def gerar_caminho_arquivo(texto_mensagem, pasta_base, identificador):
    """
    Gera o caminho completo para salvar o vídeo baseado na hierarquia do texto da mensagem.
    Linhas com:
        =   nível 1
        ==  nível 2
        === nível 3
    A primeira linha (#Fxxxx .mp4) é ignorada.
    """
	# Divide a mensagem em linhas não vazias
    linhas = [linha.strip() for linha in texto_mensagem.splitlines() if linha.strip()]

    # Remove a primeira linha se for o identificador (#Fxxxx .mp4)
    if linhas and linhas[0].startswith("#F") and ".mp4" in linhas[0]:
        linhas = linhas[1:]  # remove o identificador

	# Cria a hierarquia de pastas a partir das demais linhas
    pastas = []
    for linha in linhas:
        if linha.startswith("==="):
            pastas.append(linha.replace("===", "").strip())
        elif linha.startswith("=="):
            pastas.append(linha.replace("==", "").strip())
        elif linha.startswith("="):
            pastas.append(linha.replace("=", "").strip())
        else:
            # Linha sem "=" é o nome principal (nível raiz)
            pastas.insert(0, linha)

    # A última linha (com mais "===") é a última pasta
    ultima_pasta = pastas[-1] if pastas else identificador

    # Cria o caminho completo com base na hierarquia
    caminho_pasta = os.path.join(pasta_base, *pastas)
    criar_pasta(caminho_pasta)

	# Nome final do arquivo: "<última pasta> (<identificador>).mp4"
    nome_arquivo = f"{ultima_pasta} ({identificador}).mp4"
    caminho_final = os.path.join(caminho_pasta, nome_arquivo)

    return caminho_final

# -----------------------------------------------------
# 📥 DOWNLOAD INDIVIDUAL DO ARQUIVO
# -----------------------------------------------------
async def baixar_video(
    mensagem,
    registro,
    destino,
    semaforo,
    identificador,
    nome_arquivo,
    cliente,
    metadados_existentes,
    caminho_metadados,
    baixados,
    posicao=0
):
    """
    Efetua o download com múltiplas tentativas e barra de progresso.
    """
    global contador_baixados    # Declarando que vamos usar a variável global 'contador_baixados'
    tentativas = 0

    while tentativas < MAXIMO_TENTATIVAS:
        tentativas += 1
        pbar = None

        try:
            async with semaforo:
                caminho_temp = destino + ".part"

                # Se existir .part e for corrompido, reinicia
                if os.path.exists(caminho_temp):
                    tamanho_existente = os.path.getsize(caminho_temp)
                    tamanho_total = mensagem.media.document.size

                    if tamanho_existente > tamanho_total:
                        print(f"⚠️ .part corrompido, reiniciando: {caminho_temp}")
                        os.remove(caminho_temp)

                # Retoma download se parcialmente baixado
                offset = os.path.getsize(caminho_temp) if os.path.exists(caminho_temp) else 0
                tamanho_total_bytes = mensagem.media.document.size

                # Configuração da barra/linha de progresso
                pbar = tqdm(
                    total=tamanho_total_bytes,
                    initial=offset,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=posicao,
                    leave=True,
                    desc=f"⬇️ Baixando arquivo: {mensagem.id}|{identificador} → {nome_arquivo:<100}",
                    bar_format="{desc} {percentage:6.2f}% ({n_fmt}/{total_fmt}) [{elapsed} < {remaining}, {rate_fmt}]"
                )

                # Download em blocos
                async for bloco in cliente.iter_download(mensagem.media.document, offset=offset):
                    if bloco:
                        async with aiofiles.open(caminho_temp, "ab") as f:
                            await f.write(bloco)
                        pbar.update(len(bloco))

                pbar.close()

                # Renomeia arquivo .part → final
                os.rename(caminho_temp, destino)

                # Gera SHA1 e adiciona ao metadado (JSON)
                sha = calcular_sha1(destino)
                registro["sha1"] = sha
                registro["caminho"] = destino
                metadados_existentes.append(registro)

                # Atualiza metadado (JSON)
                async with aiofiles.open(caminho_metadados, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(metadados_existentes, ensure_ascii=False, indent=2))

                baixados.append(destino)
                print(f"\n✅ Download concluído: {mensagem.id}|{identificador} → {nome_arquivo}\n")

                contador_baixados += 1

                return destino

        except FloodWaitError as e:
            print(f"⏳ Aguardando {e.seconds}s (FloodWaitError)")
            await asyncio.sleep(e.seconds + 1)

        except TimeoutError:
            print(f"⚠️ Timeout ao baixar arquivo: {mensagem.id}|{identificador} → {nome_arquivo} ({tentativas}/{MAXIMO_TENTATIVAS} tentativas), tentando novamente em 10s...")
            await asyncio.sleep(10)

        except Exception as e:
            print(f"⚠️ Erro ao baixar arquivo: {mensagem.id}|{identificador} → {nome_arquivo} ({tentativas}/{MAXIMO_TENTATIVAS} tentativas): {e}")
            await asyncio.sleep(5)

        finally:
            if pbar:
                pbar.close()

    print(f"❌ Falha definitiva ao baixar arquivo {mensagem.id}|{identificador} → {registro.get('nome_original', '')} após {MAXIMO_TENTATIVAS} tentativas")
    return None

# -----------------------------------------------------
# 🚀 FUNÇÃO PRINCIPAL
# -----------------------------------------------------
async def main():
    criar_pasta(PASTA_DESTINO)

    cliente = TelegramClient(NOME_SESSAO, API_ID, API_HASH)
    try:
        await cliente.start()

        print("✅ Conectado como:", (await cliente.get_me()).first_name)

        data_inicial = parsear_data(DATA_INICIAL)
        data_final = parsear_data(DATA_FINAL)

        # Carrega metadados (JSON)
        caminho_metadados = os.path.join(PASTA_DESTINO, ARQUIVO_METADADOS)
        metadados_existentes = carregar_metadados(caminho_metadados)

        arquivos_existentes = {
            (m.get("nome_original"), m.get("tamanho"))
            for m in metadados_existentes if m.get("nome_original")
        }


        baixados = []
        semaforo = asyncio.Semaphore(DOWNLOADS_CONCORRENTES or 1)
        tarefas = []

        print("🔍 Buscando mensagens em", CHAT_ORIGEM)

        # -------------------------
        # ⚡ LOOP DE MENSAGENS
        # -------------------------
        async for mensagem in cliente.iter_messages(CHAT_ORIGEM, reverse=True):
            # Limite total de arquivos a serem baixados
            if LIMITE_ARQUIVOS is not None and LIMITE_ARQUIVOS > 0 and len(baixados) >= LIMITE_ARQUIVOS:
                break

            # ------------------------------
            # 🧹 FILTRA MENSAGEM VÁLIDA
            # ------------------------------
            # Processa somente mensagens válidas que contêm mídia
            if not isinstance(mensagem, Message) or not mensagem.media:
                continue
            
            # Garantir que a mídia é um documento (vídeo, arquivo, etc)
            if not isinstance(mensagem.media, MessageMediaDocument):
                continue  # ignora fotos, links, etc

            # Filtra por data
            data_mensagem = mensagem.date.astimezone(timezone.utc)
            if data_inicial and data_mensagem < data_inicial:
                continue
            if data_final and data_mensagem > data_final:
                continue

            # Ignora reenvios e respostas
            if IGNORAR_REENVIOS and mensagem.fwd_from:
                continue
            if IGNORAR_RESPOSTAS and mensagem.reply_to_msg_id:
                continue

            # Verifica se a mensagem contém o padrão  #Fxxxx
            padrao_na_mensagem = re.search(r"#F\d{4}", mensagem.text or "")
            if not padrao_na_mensagem:
                continue # pula se não tiver o padrão
            
            # Define o identificador com segurança
            identificador = padrao_na_mensagem.group(0)

            # Verifica se é vídeo
            documento = mensagem.media.document
            eh_video = any(isinstance(attr, DocumentAttributeVideo) for attr in documento.attributes)
            if SOMENTE_VIDEOS and not eh_video:
                continue

            # Filtros de tamanho do arquivo
            tamanho = getattr(documento, "size", 0)
            if TAMANHO_MINIMO and tamanho < TAMANHO_MINIMO:
                continue
            if TAMANHO_MAXIMO and TAMANHO_MAXIMO > 0 and tamanho > TAMANHO_MAXIMO:
                continue
            # FIM: FILTRA MENSAGEM VÁLIDA

            # Extrai identificador (#Fxxxx)
            identificador = mensagem.text.split()[0].strip() if mensagem.text else "sem_id"

            # Gera o caminho final conforme a hierarquia textual
            destino = gerar_caminho_arquivo(mensagem.text or "", PASTA_DESTINO, identificador)
            nome_arquivo = os.path.basename(destino)

            # Pula os arquivos duplicados (metadado)
            if EVITAR_DUPLICADOS and (nome_arquivo, tamanho) in arquivos_existentes:
                print(f"⏭️ Pulando arquivo duplicado: {nome_arquivo} ({tamanho / 1024 / 1024:.2f} MB)")
                continue

            # Registro de metadados
            registro = {
                "id_mensagem": mensagem.id,
                "data": mensagem.date.astimezone(timezone.utc).isoformat(),
                "nome_original": nome_arquivo,
                "tamanho": tamanho,
                "eh_video": eh_video,
                "legenda": mensagem.text,
            }

            # Cria tarefa assíncrona de download
            tarefas.append(asyncio.create_task(
                baixar_video(
                    mensagem,
                    registro,
                    destino,
                    semaforo,
                    identificador,
                    nome_arquivo,
                    cliente,
                    metadados_existentes,
                    caminho_metadados,
                    baixados,
                    posicao=len(tarefas)
                )
            ))
        # FIM: LOOP DE MENSAGENS

        # Aguarda todos os downloads terminarem
        if tarefas:
            await asyncio.gather(*tarefas)

        print(f"\n🏁 Downloads finalizados: {len(baixados)}. Vídeos salvos em '{PASTA_DESTINO}'")
        print(f"📘 Metadados registrados em: {caminho_metadados}")

        await cliente.disconnect()
    
    except Exception as e:
            print(f"❌ {e}")

# -------------------------
# ▶️ EXECUÇÃO DO SCRIPT
# -------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n⚠️ Execução interrompida pelo usuário. {contador_baixados} arquivo(s) baixado(s).")
