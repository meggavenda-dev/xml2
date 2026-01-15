
# file: tiss_parser.py
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import IO, Union, List, Dict
import xml.etree.ElementTree as ET

# Namespace TISS
ANS_NS = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}

__version__ = "2026.01.15-ptbr-07"


class TissParsingError(Exception):
    """Erro de parsing para arquivos TISS XML."""
    pass


# ----------------------------
# Helpers
# ----------------------------
def _dec(txt: str | None) -> Decimal:
    """
    Converte string numérica para Decimal; vazio/None => 0.
    Troca ',' por '.' por segurança.
    """
    if not txt:
        return Decimal('0')
    return Decimal(txt.strip().replace(',', '.'))


def _get_text(root_or_el: ET.Element, xpath: str) -> str:
    """
    Retorna texto de um xpath (com namespace TISS),
    ou string vazia se não existir / sem texto.
    """
    el = root_or_el.find(xpath, ANS_NS)
    return (el.text or '').strip() if el is not None and el.text else ''


def _is_consulta(root: ET.Element) -> bool:
    """True se houver guiaConsulta."""
    return root.find('.//ans:guiaConsulta', ANS_NS) is not None


def _is_sadt(root: ET.Element) -> bool:
    """True se houver guiaSP-SADT."""
    return root.find('.//ans:guiaSP-SADT', ANS_NS) is not None


def _is_recurso(root: ET.Element) -> bool:
    """
    True se for RECURSO_GLOSA, identificado pelo tipoTransacao
    ou pela presença de guiaRecursoGlosa.
    """
    tipo = root.findtext('.//ans:cabecalho/ans:identificacaoTransacao/ans:tipoTransacao', namespaces=ANS_NS)
    if (tipo or '').strip().upper() == 'RECURSO_GLOSA':
        return True
    return root.find('.//ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa', ANS_NS) is not None


def _get_numero_lote(root: ET.Element) -> str:
    """
    Extrai numeroLote:
      - Lote de guias (Consulta / SADT)
      - Recurso de glosa (guiaRecursoGlosa/numeroLote)
    """
    # 1) Consulta / SADT
    el = root.find('.//ans:prestadorParaOperadora/ans:loteGuias/ans:numeroLote', ANS_NS)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()

    # 2) Recurso de glosa
    el = root.find('.//ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa/ans:numeroLote', ANS_NS)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()

    raise TissParsingError('numeroLote não encontrado no XML.')


# ----------------------------
# CONSULTA
# ----------------------------
def _sum_consulta(root: ET.Element) -> tuple[int, Decimal, str]:
    """
    Soma ans:procedimento/ans:valorProcedimento por ans:guiaConsulta.
    Estratégia: 'consulta_valorProcedimento'
    """
    total = Decimal('0')
    guias = root.findall('.//ans:prestadorParaOperadora/ans:loteGuias/ans:guiasTISS/ans:guiaConsulta', ANS_NS)
    for g in guias:
        val_el = g.find('.//ans:procedimento/ans:valorProcedimento', ANS_NS)
        total += _dec(val_el.text if val_el is not None else None)
    return len(guias), total, "consulta_valorProcedimento"


# ----------------------------
# SADT - soma por guia (robusta)
# ----------------------------
def _sum_itens_procedimentos(guia: ET.Element) -> Decimal:
    total = Decimal('0')
    for it in guia.findall('.//ans:procedimentosExecutados/ans:procedimentoExecutado', ANS_NS):
        vtot = it.find('ans:valorTotal', ANS_NS)
        if vtot is not None and vtot.text and vtot.text.strip():
            total += _dec(vtot.text)
        else:
            vuni = it.find('ans:valorUnitario', ANS_NS)
            qtd  = it.find('ans:quantidadeExecutada', ANS_NS)
            if (vuni is not None and vuni.text) and (qtd is not None and qtd.text):
                total += _dec(vuni.text) * _dec(qtd.text)
    return total


def _sum_itens_outras_desp(guia: ET.Element) -> Decimal:
    total = Decimal('0')
    for desp in guia.findall('.//ans:outrasDespesas/ans:despesa', ANS_NS):
        sv = desp.find('ans:servicosExecutados', ANS_NS)
        if sv is None:
            continue
        el_val = sv.find('ans:valorTotal', ANS_NS)
        total += _dec(el_val.text if el_val is not None else None)
    return total


def _sum_componentes_valorTotal(guia: ET.Element) -> Decimal:
    """
    Soma componentes do bloco valorTotal da guia:
    valorProcedimentos, valorDiarias, valorTaxasAlugueis,
    valorMateriais, valorMedicamentos, valorGasesMedicinais
    """
    total = Decimal('0')
    vt = guia.find('ans:valorTotal', ANS_NS)  # bloco da guia
    if vt is None:
        return Decimal('0')
    for tag in ('valorProcedimentos', 'valorDiarias', 'valorTaxasAlugueis',
                'valorMateriais', 'valorMedicamentos', 'valorGasesMedicinais'):
        el = vt.find(f'ans:{tag}', ANS_NS)
        total += _dec(el.text if el is not None else None)
    return total


def _sum_sadt_guia(guia: ET.Element) -> tuple[Decimal, str]:
    """
    Estratégia:
      1) Tenta ans:valorTotal/ans:valorTotalGeral (do bloco da guia).
      2) Senão, soma ITENS (procedimentos + outras despesas).
      3) Por último, soma COMPONENTES do valorTotal (quando existir).
    """
    # 1) valorTotalGeral (bloco da guia, sem //)
    vt = guia.find('ans:valorTotal', ANS_NS)
    if vt is not None:
        vtg = vt.find('ans:valorTotalGeral', ANS_NS)
        vtg_val = _dec(vtg.text if vtg is not None else None)
        if vtg_val > 0:
            return vtg_val, 'valorTotalGeral'

    # 2) Itens
    proc_itens = _sum_itens_procedimentos(guia)
    outras_itens = _sum_itens_outras_desp(guia)
    itens_total = proc_itens + outras_itens
    if itens_total > 0:
        return itens_total, 'itens (proced+outras)'

    # 3) Componentes
    comp_total = _sum_componentes_valorTotal(guia)
    if comp_total > 0:
        return comp_total, 'componentes_valorTotal'

    return Decimal('0'), 'zero'


def _sum_sadt(root: ET.Element) -> tuple[int, Decimal, str]:
    total = Decimal('0')
    guias = root.findall('.//ans:prestadorParaOperadora/ans:loteGuias/ans:guiasTISS/ans:guiaSP-SADT', ANS_NS)
    estrategias: Dict[str, int] = {}

    for g in guias:
        v, strat = _sum_sadt_guia(g)
        total += v
        estrategias[strat] = estrategias.get(strat, 0) + 1

    if not guias:
        return 0, Decimal('0'), 'zero'

    if len(estrategias) == 1:
        estrategia_arquivo = next(iter(estrategias.keys()))
    else:
        estrategia_arquivo = "misto: " + ", ".join(
            f"{k}={v}" for k, v in sorted(estrategias.items(), key=lambda x: (-x[1], x[0]))
        )
    return len(guias), total, estrategia_arquivo


# ----------------------------
# RECURSO DE GLOSA
# ----------------------------
def _sum_recurso(root: ET.Element) -> tuple[int, Decimal, str, str]:
    """
    Recurso de glosa:
      - qtde_guias = quantidade de 'recursoGuia'
      - valor_total = 'valorTotalRecursado' (do bloco guiaRecursoGlosa)
      - protocolo = 'numeroProtocolo' (do bloco guiaRecursoGlosa)
      - estratégia = 'recurso_valorTotalRecursado'
    """
    base = './/ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa'
    qtde_guias = len(root.findall(f'{base}/ans:opcaoRecurso/ans:recursoGuia', ANS_NS))
    valor_total = _dec(_get_text(root, f'{base}/ans:valorTotalRecursado'))
    protocolo = _get_text(root, f'{base}/ans:numeroProtocolo') or ''
    return qtde_guias, valor_total, 'recurso_valorTotalRecursado', protocolo


# ----------------------------
# API pública
# ----------------------------
def _parse_root(root: ET.Element, arquivo_nome: str) -> Dict:
    numero_lote = _get_numero_lote(root)

    # RECURSO
    if _is_recurso(root):
        n_guias, total, estrategia, protocolo = _sum_recurso(root)
        out = {
            'arquivo': arquivo_nome,
            'numero_lote': numero_lote,
            'tipo': 'RECURSO',
            'qtde_guias': n_guias,
            'valor_total': total,               # valor recursado (no contexto de recurso)
            'valor_glosado': Decimal('0'),      # preencher via Demonstrativo
            'valor_liberado': Decimal('0'),     # preencher via Demonstrativo
            'estrategia_total': estrategia,
            'parser_version': __version__,
        }
        if protocolo:
            out['protocolo'] = protocolo
        return out

    # CONSULTA
    if _is_consulta(root):
        n_guias, total, estrategia = _sum_consulta(root)
        return {
            'arquivo': arquivo_nome,
            'numero_lote': numero_lote,
            'tipo': 'CONSULTA',
            'qtde_guias': n_guias,
            'valor_total': total,               # valor APRESENTADO
            'valor_glosado': Decimal('0'),      # preencher via Demonstrativo
            'valor_liberado': Decimal('0'),     # preencher via Demonstrativo
            'estrategia_total': estrategia,
            'parser_version': __version__,
        }

    # SADT (ou DESCONHECIDO)
    tipo = 'SADT' if _is_sadt(root) else 'DESCONHECIDO'
    n_guias, total, estrategia = _sum_sadt(root) if tipo == 'SADT' else (0, Decimal('0'), 'zero')
    return {
        'arquivo': arquivo_nome,
        'numero_lote': numero_lote,
        'tipo': tipo,
        'qtde_guias': n_guias,
        'valor_total': total,                   # valor APRESENTADO
        'valor_glosado': Decimal('0'),          # preencher via Demonstrativo
        'valor_liberado': Decimal('0'),         # preencher via Demonstrativo
        'estrategia_total': estrategia,
        'parser_version': __version__,
    }


def parse_tiss_xml(source: Union[str, Path, IO[bytes]]) -> Dict:
    """
    Lê um XML TISS a partir de caminho (str/Path) OU arquivo (IO[bytes]/BytesIO).
    Suporta: CONSULTA, SP-SADT e RECURSO_GLOSA.
    """
    if hasattr(source, 'read'):  # UploadedFile/BytesIO
        try:
            if hasattr(source, 'seek'):
                source.seek(0)
        except Exception:
            pass
        root = ET.parse(source).getroot()
        arquivo_nome = getattr(source, 'name', 'upload.xml')
        return _parse_root(root, Path(arquivo_nome).name)

    path = Path(source)
    root = ET.parse(path).getroot()
    return _parse_root(root, path.name)


def parse_many_xmls(paths: List[Union[str, Path]]) -> List[Dict]:
    """
    Lê vários XMLs, retornando uma lista de dicionários (um por arquivo).
    Em caso de erro, retorna um dict com 'erro' preenchido.
    """
    resultados: List[Dict] = []
    for p in paths:
        try:
            resultados.append(parse_tiss_xml(p))
        except Exception as e:
            resultados.append({
                'arquivo': Path(p).name if hasattr(p, 'name') else str(p),
                'numero_lote': '',
                'tipo': 'DESCONHECIDO',
                'qtde_guias': 0,
                'valor_total': Decimal('0'),
                'valor_glosado': Decimal('0'),
                'valor_liberado': Decimal('0'),
                'estrategia_total': 'erro',
                'parser_version': __version__,
                'erro': str(e),
            })
    return resultados


# ----------------------------
# Auditoria por guia (opcional)
# ----------------------------
def audit_por_guia(source: Union[str, Path, IO[bytes]]) -> List[Dict]:
    """
    Uma linha por guia:
      - Para RECURSO: numeroGuiaOrigem, numeroGuiaOperadora, senha,
                      codGlosaGuia, justificativa_prefix, numero_lote, protocolo.
      - Para CONSULTA: numeroGuiaPrestador e valor (valorProcedimento).
      - Para SADT: numeroGuiaPrestador, total_tag (valorTotalGeral),
                   subtotais por itens e soma (procedimentos/outras).
    """
    # Carrega XML
    if hasattr(source, 'read'):
        try:
            if hasattr(source, 'seek'):
                source.seek(0)
        except Exception:
            pass
        root = ET.parse(source).getroot()
        arquivo_nome = getattr(source, 'name', 'upload.xml')
    else:
        p = Path(source)
        root = ET.parse(p).getroot()
        arquivo_nome = p.name

    out: List[Dict] = []

    # Tenta capturar numero_lote para registrar nas linhas de auditoria
    numero_lote_for_audit = ""
    try:
        numero_lote_for_audit = _get_numero_lote(root)
    except Exception:
        numero_lote_for_audit = ""

    # RECURSO
    if _is_recurso(root):
        base = './/ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa'
        protocolo = _get_text(root, f'{base}/ans:numeroProtocolo')
        lote = _get_text(root, f'{base}/ans:numeroLote')
        for rg in root.findall(f'{base}/ans:opcaoRecurso/ans:recursoGuia', ANS_NS):
            num_origem = _get_text(rg, 'ans:numeroGuiaOrigem')
            num_oper   = _get_text(rg, 'ans:numeroGuiaOperadora')
            senha      = _get_text(rg, 'ans:senha')
            cod_glosa  = _get_text(rg, './/ans:recursoGuiaCompleta/ans:codGlosaGuia')
            just       = _get_text(rg, './/ans:recursoGuiaCompleta/ans:justificativaGuia')
            out.append({
                'arquivo': arquivo_nome,
                'tipo': 'RECURSO',
                'numero_lote': (lote or numero_lote_for_audit or ''),
                'protocolo': protocolo,
                'numeroGuiaOrigem': num_origem,
                'numeroGuiaOperadora': num_oper,
                'senha': senha,
                'codGlosaGuia': cod_glosa,
                'justificativa_prefix': (just[:250] + '…') if just else '',
                'parser_version': __version__,
            })
        return out

    # CONSULTA
    if _is_consulta(root):
        for g in root.findall('.//ans:guiaConsulta', ANS_NS):
            vp = g.find('.//ans:procedimento/ans:valorProcedimento', ANS_NS)
            v = _dec(vp.text if vp is not None else None)
            out.append({
                'arquivo': arquivo_nome,
                'tipo': 'CONSULTA',
                'numeroGuiaPrestador': (g.find('.//ans:numeroGuiaPrestador', ANS_NS).text.strip()
                                        if g.find('.//ans:numeroGuiaPrestador', ANS_NS) is not None else ''),
                'total_tag': v,
                'subtotal_itens_proc': v,
                'subtotal_itens_outras': Decimal('0'),
                'subtotal_itens': v,
                'numero_lote': numero_lote_for_audit,
                'parser_version': __version__,
            })
        return out

    # SADT
    for g in root.findall('.//ans:guiaSP-SADT', ANS_NS):
        cab = g.find('.//ans:cabecalhoGuia', ANS_NS)
        num_prest = (cab.find('ans:numeroGuiaPrestador', ANS_NS).text.strip()
                     if cab is not None and cab.find('ans:numeroGuiaPrestador', ANS_NS) is not None else '')
        vt = g.find('ans:valorTotal', ANS_NS)  # sem //
        vtg = _dec(vt.find('ans:valorTotalGeral', ANS_NS).text) if (vt is not None and vt.find('ans:valorTotalGeral', ANS_NS) is not None) else Decimal('0')
        proc = _sum_itens_procedimentos(g)
        outras = _sum_itens_outras_desp(g)
        out.append({
            'arquivo': arquivo_nome,
            'tipo': 'SADT',
            'numeroGuiaPrestador': num_prest,
            'total_tag': vtg,
            'subtotal_itens_proc': proc,
            'subtotal_itens_outras': outras,
            'subtotal_itens': proc + outras,
            'numero_lote': numero_lote_for_audit,
            'parser_version': __version__,
        })
    return out
