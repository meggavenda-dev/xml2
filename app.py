
from __future__ import annotations

import io
import re
import math
from decimal import Decimal
from pathlib import Path
from typing import List, Dict

import pandas as pd
import streamlit as st

from tiss_parser import (
    parse_tiss_xml,
    parse_many_xmls,
    audit_por_guia,
    __version__ as PARSER_VERSION
)

st.set_page_config(page_title="Leitor TISS XML (Consulta • SADT • Recurso)", layout="wide")
st.title("Leitor de XML TISS (Consulta, SP‑SADT e Recurso de Glosa)")
st.caption(f"Extrai nº do lote, protocolo, quantidade de guias e valor total • Parser {PARSER_VERSION}")

tab1, tab2 = st.tabs(["Upload de XML(s)", "Ler de uma pasta local"])

def format_currency_br(val) -> str:
    try:
        v = float(Decimal(str(val)))
    except Exception:
        v = 0.0
    if not math.isfinite(v):
        v = 0.0
    neg = v < 0
    v = abs(v)
    inteiro = int(v)
    centavos = int(round((v - inteiro) * 100))
    inteiro_fmt = f"{inteiro:,}".replace(",", ".")
    centavos_fmt = f"{centavos:02d}"
    s = f"R$ {inteiro_fmt},{centavos_fmt}"
    return f"-{s}" if neg else s

def _df_display_currency(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    dfd = df.copy()
    for c in cols:
        if c in dfd.columns:
            dfd[c] = dfd[c].apply(format_currency_br)
    return dfd

_LOTE_REGEX = re.compile(r'(?i)lote\s*[-_]*\s*(\d+)')

def extract_lote_from_filename(name: str) -> str | None:
    if not isinstance(name, str):
        return None
    m = _LOTE_REGEX.search(name)
    if m:
        return m.group(1)
    return None

def _to_float(val) -> float:
    try:
        return float(Decimal(str(val)))
    except Exception:
        return 0.0

def _df_format(df: pd.DataFrame) -> pd.DataFrame:
    if 'valor_total' in df.columns:
        df['valor_total'] = df['valor_total'].apply(_to_float)
    if 'qtde_guias' in df.columns and 'valor_total' in df.columns:
        df['suspeito'] = (df['qtde_guias'] > 0) & (df['valor_total'] == 0)
    else:
        df['suspeito'] = False
    if 'protocolo' not in df.columns:
        df['protocolo'] = None
    df['lote_arquivo'] = df['arquivo'].apply(extract_lote_from_filename)
    df['lote_arquivo_int'] = pd.to_numeric(df['lote_arquivo'], errors='coerce').astype('Int64')
    if 'numero_lote' in df.columns:
        df['lote_confere'] = (df['lote_arquivo'].fillna('') == df['numero_lote'].fillna(''))
    else:
        df['lote_confere'] = pd.NA
    if 'erro' not in df.columns:
        df['erro'] = None
    for c in ('valor_glosado', 'valor_liberado'):
        if c not in df.columns:
            df[c] = 0.0
    ordenar = ['numero_lote','protocolo','tipo','qtde_guias','valor_total','valor_glosado','valor_liberado','estrategia_total','arquivo','lote_arquivo','lote_arquivo_int','lote_confere','suspeito','erro','parser_version']
    cols = [c for c in ordenar if c in df.columns] + [c for c in df.columns if c not in ordenar]
    df = df[cols].sort_values(['numero_lote','tipo','arquivo'], ignore_index=True)
    return df

def _make_agg(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['numero_lote','tipo','qtde_arquivos','qtde_guias_total','valor_total'])
    agg = df.groupby(['numero_lote','tipo'], dropna=False, as_index=False).agg(qtde_arquivos=('arquivo','count'),qtde_guias_total=('qtde_guias','sum'),valor_total=('valor_total','sum')).sort_values(['numero_lote','tipo'], ignore_index=True)
    return agg

with tab1:
    files = st.file_uploader("Selecione XML(s) TISS", type=['xml'], accept_multiple_files=True)
    if files:
        resultados: List[Dict] = []
        for f in files:
            try:
                if hasattr(f,'seek'): f.seek(0)
                res = parse_tiss_xml(f)
                res['arquivo'] = f.name
                if 'erro' not in res: res['erro'] = None
            except Exception as e:
                res = {'arquivo':f.name,'numero_lote':'','tipo':'DESCONHECIDO','qtde_guias':0,'valor_total':Decimal('0'),'estrategia_total':'erro','parser_version':PARSER_VERSION,'erro':str(e)}
            resultados.append(res)

        if resultados:
            df = pd.DataFrame(resultados)
            df = _df_format(df)
            st.subheader("Resumo por arquivo")
            st.dataframe(_df_display_currency(df,['valor_total','valor_glosado','valor_liberado']), use_container_width=True)
            st.subheader("Agregado por lote e tipo")
            agg = _make_agg(df)
            st.dataframe(_df_display_currency(agg,['valor_total']), use_container_width=True)

            # Auditoria por guia
            with st.expander("🔎 Auditoria por guia"):
                arquivo_escolhido = st.selectbox("Selecione um arquivo", options=[r['arquivo'] for r in resultados], key="auditoria_select")
                if st.button("Gerar auditoria", key="auditoria_btn"):
                    escolhido = next((f for f in files if f.name == arquivo_escolhido), None)
                    if escolhido:
                        if hasattr(escolhido,'seek'): escolhido.seek(0)
                        linhas = audit_por_guia(escolhido)
                        df_a = pd.DataFrame(linhas)
                        df_a_disp = df_a.copy()
                        for c in ('total_tag','subtotal_itens_proc','subtotal_itens_outras','subtotal_itens'):
                            if c in df_a_disp.columns:
                                df_a_disp[c] = df_a_disp[c].apply(format_currency_br)
                        st.dataframe(df_a_disp, use_container_width=True)

            # Comparar XML e remover duplicadas
            with st.expander("🧩 Comparar XML e remover duplicadas"):
                arquivo_base = st.selectbox("Arquivo base", options=[r['arquivo'] for r in resultados], key="comparar_select")
                if st.button("Remover duplicadas", key="comparar_btn"):
                    base_file = next((f for f in files if f.name == arquivo_base), None)
                    outros_files = [f for f in files if f.name != arquivo_base]
                    if base_file and outros_files:
                        if hasattr(base_file,'seek'): base_file.seek(0)
                        guias_base = audit_por_guia(base_file)
                        guias_outros = []
                        for f in outros_files:
                            if hasattr(f,'seek'): f.seek(0)
                            guias_outros.extend(audit_por_guia(f))
                        duplicadas = []
                        for g in guias_base:
                            chave = g.get('numeroGuiaPrestador') if g['tipo'] in ('CONSULTA','SADT') else g.get('numeroGuiaOrigem') or g.get('numeroGuiaOperadora')
                            if chave:
                                for o in guias_outros:
                                    chave_outro = o.get('numeroGuiaPrestador') if o['tipo'] in ('CONSULTA','SADT') else o.get('numeroGuiaOrigem') or o.get('numeroGuiaOperadora')
                                    if chave_outro == chave:
                                        duplicadas.append(g)
                                        break
                        if duplicadas:
                            st.warning(f"{len(duplicadas)} duplicadas encontradas.")
                            df_dup = pd.DataFrame(duplicadas)
                            st.dataframe(df_dup[['arquivo','tipo','numeroGuiaPrestador','numeroGuiaOrigem','numeroGuiaOperadora','numero_lote']], use_container_width=True)
                            from lxml import etree
                            base_file.seek(0)
                            parser = etree.XMLParser(remove_blank_text=True)
                            tree = etree.parse(base_file, parser)
                            root = tree.getroot()
                            def remover_guias(root, duplicadas):
                                for dup in duplicadas:
                                    tipo = dup['tipo']
                                    chave = dup.get('numeroGuiaPrestador') or dup.get('numeroGuiaOrigem') or dup.get('numeroGuiaOperadora')
                                    if not chave: continue
                                    if tipo=='CONSULTA':
                                        for guia in root.xpath('.//ans:guiaConsulta', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'}):
                                            num = guia.find('.//ans:numeroGuiaPrestador', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'})
                                            if num is not None and (num.text or '').strip()==chave:
                                                guia.getparent().remove(guia)
                                    elif tipo=='SADT':
                                        for guia in root.xpath('.//ans:guiaSP-SADT', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'}):
                                            num = guia.find('.//ans:cabecalhoGuia/ans:numeroGuiaPrestador', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'})
                                            if num is not None and (num.text or '').strip()==chave:
                                                guia.getparent().remove(guia)
                                    elif tipo=='RECURSO':
                                        for guia in root.xpath('.//ans:recursoGuia', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'}):
                                            num = guia.find('.//ans:numeroGuiaOrigem', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'})
                                            num2 = guia.find('.//ans:numeroGuiaOperadora', namespaces={'ans':'http://www.ans.gov.br/padroes/tiss/schemas'})
                                            if ((num is not None and (num.text or '').strip()==chave) or (num2 is not None and (num2.text or '').strip()==chave)):
                                                guia.getparent().remove(guia)
                                return root
                            root = remover_guias(root, duplicadas)
                            buffer_xml = io.BytesIO()
                            tree.write(buffer_xml, encoding='utf-8', xml_declaration=True, pretty_print=True)
                            st.download_button("Baixar XML sem duplicadas", data=buffer_xml.getvalue(), file_name=f"{arquivo_base.replace('.xml','')}_sem_duplicadas.xml", mime="application/xml", key="comparar_download")

            # Verificar retornos de consulta por período
            with st.expander("🔍 Verificar retornos de consulta por período"):
                periodo = st.number_input("Informe o período em dias", min_value=1, value=30, key="retorno_periodo")
                if st.button("Verificar retornos", key="retorno_btn"):
                    todas_guias = []
                    for f in files:
                        if hasattr(f,'seek'): f.seek(0)
                        todas_guias.extend(audit_por_guia(f))
                    df_all = pd.DataFrame(todas_guias)
                    df_consultas = df_all[df_all['tipo']=='CONSULTA'].copy()
                    df_consultas['dataAtendimento'] = pd.to_datetime(df_consultas['dataAtendimento'], errors='coerce')
                    retornos = []
                    for (paciente, profissional), grupo in df_consultas.groupby(['paciente_id','profissional_id']):
                        grupo = grupo.sort_values('dataAtendimento')
                        datas = grupo['dataAtendimento'].tolist()
                        arquivos = grupo['arquivo'].tolist()
                        lotes = grupo['numero_lote'].tolist()
                        for i in range(len(datas)-1):
                            if pd.notnull(datas[i]) and pd.notnull(datas[i+1]):
                                diff = abs((datas[i+1]-datas[i]).days)
                                if diff <= periodo:
                                    retornos.append({'paciente_id':paciente,'profissional_id':profissional,'data_consulta':datas[i],'data_retorno':datas[i+1],'dif_dias':diff,'arquivo_consulta':arquivos[i],'arquivo_retorno':arquivos[i+1],'lote_consulta':lotes[i],'lote_retorno':lotes[i+1]})
                    if retornos:
                        df_ret = pd.DataFrame(retornos)
                        st.dataframe(df_ret, use_container_width=True)
                    else:
                        st.info("Nenhum retorno encontrado no período informado.")
