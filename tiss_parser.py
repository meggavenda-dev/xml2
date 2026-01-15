
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import IO, Union, List, Dict
import xml.etree.ElementTree as ET

# Namespace TISS
ANS_NS = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}

__version__ = "2026.01.15-ptbr-08"


class TissParsingError(Exception):
    """Erro de parsing para arquivos TISS XML."""
    pass


# ----------------------------
# Helpers
# ----------------------------
def _dec(txt: str | None) -> Decimal:
    """Converte string numérica para Decimal; vazio/None => 0. Troca ',' por '.' por segurança."""
    if not txt:
        return Decimal('0')
    return Decimal(txt.strip().replace(',', '.'))


def _get_text(root_or_el: ET.Element, xpath: str) -> str:
    """Retorna texto de um xpath (com namespace TISS), ou string vazia se não existir / sem texto."""
    el = root_or_el.find(xpath, ANS_NS)
    return (el.text or '').strip() if el is not None and el.text else ''


def _is_consulta(root: ET.Element) -> bool:
    return root.find('.//ans:guiaConsulta', ANS_NS) is not None


def _is_sadt(root: ET.Element) -> bool:
    return root.find('.//ans:guiaSP-SADT', ANS_NS) is not None


def _is_recurso(root: ET.Element) -> bool:
    tipo = root.findtext('.//ans:cabecalho/ans:identificacaoTransacao/ans:tipoTransacao', namespaces=ANS_NS)
    if (tipo or '').strip().upper() == 'RECURSO_GLOSA':
        return True
    return root.find('.//ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa', ANS_NS) is not None


def _get_numero_lote(root: ET.Element) -> str:
    el = root.find('.//ans:prestadorParaOperadora/ans:loteGuias/ans:numeroLote', ANS_NS)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()
    el = root.find('.//ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa/ans:numeroLote', ANS_NS)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()
    raise TissParsingError('numeroLote não encontrado no XML.')


# ----------------------------
# CONSULTA
# ----------------------------
def _sum_consulta(root: ET.Element) -> tuple[int, Decimal, str]:
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
            qtd = it.find('ans:quantidadeExecutada', ANS_NS)
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
    total = Decimal('0')
    vt = guia.find('ans:valorTotal', ANS_NS)
    if vt is None:
        return Decimal('0')
    for tag in ('valorProcedimentos', 'valorDiarias', 'valorTaxasAlugueis',
                'valorMateriais', 'valorMedicamentos', 'valorGasesMedicinais'):
        el = vt.find(f'ans:{tag}', ANS_NS)
        total += _dec(el.text if el is not None else None)
    return total


def _sum_sadt_guia(guia: ET.Element) -> tuple[Decimal, str]:
    vt = guia.find('ans:valorTotal', ANS_NS)
    if vt is not None:
        vtg = vt.find('ans:valorTotalGeral', ANS_NS)
        vtg_val = _dec(vtg.text if vtg is not None else None)
        if vtg_val > 0:
            return vtg_val, 'valorTotalGeral'

    proc_itens = _sum_itens_procedimentos(guia)
    outras_itens = _sum_itens_outras_desp(guia)
    itens_total = proc_itens + outras_itens
    if itens_total > 0:
        return itens_total, 'itens (proced+outras)'

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

    if _is_recurso(root):
        n_guias, total, estrategia, protocolo = _sum_recurso(root)
        out = {
            'arquivo': arquivo_nome,
            'numero_lote': numero_lote,
            'tipo': 'RECURSO',
            'qtde_guias': n_guias,
            'valor_total': total,
            'valor_glosado': Decimal('0'),
            'valor_liberado': Decimal('0'),
            'estrategia_total': estrategia,
            'parser_version': __version__,
        }
        if protocolo:
            out['protocolo'] = protocolo
        return out

    if _is_consulta(root):
        n_guias, total, estrategia = _sum_consulta(root)
        return {
            'arquivo': arquivo_nome,
            'numero_lote': numero_lote,
            'tipo': 'CONSULTA',
            'qtde_guias': n_guias,
            'valor_total': total,
            'valor_glosado': Decimal('0'),
            'valor_liberado': Decimal('0'),
            'estrategia_total': estrategia,
            'parser_version': __version__,
        }

    tipo = 'SADT' if _is_sadt(root) else 'DESCONHECIDO'
    n_guias, total, estrategia = _sum_sadt(root) if tipo == 'SADT' else (0, Decimal('0'), 'zero')
    return {
        'arquivo': arquivo_nome,
        'numero_lote': numero_lote,
        'tipo': tipo,
        'qtde_guias': n_guias,
        'valor_total': total,
        'valor_glosado': Decimal('0'),
        'valor_liberado': Decimal('0'),
        'estrategia_total': estrategia,
        'parser_version': __version__,
    }


def parse_tiss_xml(source: Union[str, Path, IO[bytes]]) -> Dict:
    if hasattr(source, 'read'):
        if hasattr(source, 'seek'):
            source.seek(0)
        root = ET.parse(source).getroot()
        arquivo_nome = getattr(source, 'name', 'upload.xml')
        return _parse_root(root, Path(arquivo_nome).name)

    path = Path(source)
    root = ET.parse(path).getroot()
    return _parse_root(root, path.name)


def parse_many_xmls(paths: List[Union[str, Path]]) -> List[Dict]:
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
# Auditoria por guia (com paciente, médico e data)
# ----------------------------
def audit_por_guia(source: Union[str, Path, IO[bytes]]) -> List[Dict]:
    if hasattr(source, 'read'):
        if hasattr(source, 'seek'):
            source.seek(0)
        root = ET.parse(source).getroot()
        arquivo_nome = getattr(source, 'name', 'upload.xml')
    else:
        p = Path(source)
        root = ET.parse(p).getroot()
        arquivo_nome = p.name

    out: List[Dict] = []
    numero_lote_for_audit = ""
    try:
        numero_lote_for_audit = _get_numero_lote(root)
    except Exception:
        numero_lote_for_audit = ""

    def _get_paciente(g: ET.Element) -> str:
        el = g.find('.//ans:dadosBeneficiario/ans:nomeBeneficiario', ANS_NS)
        return (el.text or '').strip() if el is not None else ''

    def _get_medico(g: ET.Element) -> str:
        el = g.find('.//ans:dadosProfissionaisResponsaveis/ans:nomeProfissional', ANS_NS)
        return (el.text or '').strip() if el is not None else ''

    def _get_data(g: ET.Element) -> str:
        el = g.find('.//ans:dataAtendimento', ANS_NS)
        return (el.text or '').strip() if el is not None else ''

    # RECURSO
    if _is_recurso(root):
        base = './/ans:prestadorParaOperadora/ans:recursoGlosa/ans:guiaRecursoGlosa'
        protocolo = _get_text(root, f'{base}/ans:numeroProtocolo')
        lote = _get_text(root, f'{base}/ans:numeroLote')
        for rg in root.findall(f'{base}/ans:opcaoRecurso/ans:recursoGuia', ANS_NS):
            out.append({
                'arquivo': arquivo_nome,
                'tipo': 'RECURSO',
                'numero_lote': (lote or numero_lote_for_audit or ''),
                'protocolo': protocolo,
                'numeroGuiaOrigem': _get_text(rg, 'ans:numeroGuiaOrigem'),
                'numeroGuiaOperadora': _get_text(rg, 'ans:numeroGuiaOperadora'),
                'paciente': _get_paciente(rg),
                'medico': _get_medico(rg),
                'data_atendimento': _get_data(rg),
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
                'numeroGuiaPrestador': _get_text(g, 'ans:numeroGuiaPrestador'),
                'paciente': _get_paciente(g),
                'medico': _get_medico(g),
                'data_atendimento': _get_data(g),
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
        vt = g.find('ans:valorTotal', ANS_NS)
        vtg = _dec(vt.find('ans:valorTotalGeral', ANS_NS).text) if (vt is not None and vt.find('ans:valorTotalGeral', ANS_NS) is not None) else Decimal('0')
        proc = _sum_itens_procedimentos(g)
        outras = _sum_itens_outras_desp(g)
        out.append({
            'arquivo': arquivo_nome,
            'tipo': 'SADT',
            'numeroGuiaPrestador': num_prest,
            'paciente': _get_paciente(g),
            'medico': _get_medico(g),
            'data_atendimento': _get_data(g),
            'total_tag': vtg,
            'subtotal_itens_proc': proc,
            'subtotal_itens_outras': outras,
            'subtotal_itens': proc + outras,
            'numero_lote': numero_lote_for_audit,
            'parser_version': __version__,
        })
    return out
