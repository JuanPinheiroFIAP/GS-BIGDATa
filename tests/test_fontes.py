"""Testes da lógica de coleta (sem rede, sem Airflow). Rodar:  python -m unittest discover -s tests -t ."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gs_big_data import arquivos
from gs_big_data.fontes import lugares, osm, relatos

JS_EXEMPLO = """
/** comentário */
export const PLACES = [
  {
    id: 'cafe-aurora',
    name: 'Café Aurora',
    lat: -23.5631,
    lng: -46.6522,
    features: ['ramp', 'braille'],
    reports: [{ need: 'vision', author: 'Paulo', text: "Cão-guia bem recebido.", daysAgo: 12 }],
  },
  { id: 'outro', name: 'Outro', lat: -23.5, lng: -46.6, features: [], },
];

export const findPlace = (id) => PLACES.find((place) => place.id === id);
"""

CATALOGO = [
    {"id": "a", "features": ["ramp", "elevator", "restroom", "parking"]},
    {"id": "b", "features": []},
    {"id": "c", "features": ["libras", "captions", "braille"]},
]


class ExtrairLugaresTest(unittest.TestCase):
    def test_le_objeto_javascript(self):
        achados = lugares.extrair_lugares(JS_EXEMPLO)
        self.assertEqual([p["id"] for p in achados], ["cafe-aurora", "outro"])
        self.assertEqual(achados[0]["reports"][0]["need"], "vision")

    def test_sem_places_levanta_erro(self):
        with self.assertRaises(ValueError):
            lugares.extrair_lugares("export const OUTRA = [];")


class RelatosTest(unittest.TestCase):
    def test_mesma_data_gera_o_mesmo_resultado(self):
        self.assertEqual(relatos.gerar("2026-11-14", CATALOGO), relatos.gerar("2026-11-14", CATALOGO))

    def test_datas_diferentes_geram_relatos_diferentes(self):
        a, _ = relatos.gerar("2026-11-14", CATALOGO)
        b, _ = relatos.gerar("2026-11-15", CATALOGO)
        self.assertNotEqual([r["texto"] for r in a], [r["texto"] for r in b])
        self.assertTrue(all(r["relato_id"].startswith("2026-11-14-") for r in a))

    def test_sem_sujeira_os_dados_sao_validos(self):
        registros, gabarito = relatos.gerar("2026-11-14", CATALOGO, quantidade=200, taxa_sujeira=0)
        self.assertEqual(gabarito, {})
        self.assertEqual(len(registros), 200)
        ids = {p["id"] for p in CATALOGO}
        for r in registros:
            self.assertIn(r["lugar_id"], ids)
            self.assertIn(r["necessidade"], relatos.NECESSIDADES)
            self.assertIn(r["nota"], range(1, 6))
            self.assertTrue(r["autor_email"].endswith("@example.com"))

    def test_gabarito_bate_com_o_arquivo(self):
        registros, gabarito = relatos.gerar("2026-11-14", CATALOGO, quantidade=300, taxa_sujeira=0.3)
        self.assertGreater(sum(gabarito.values()), 0)
        self.assertEqual(len(registros), 300 + gabarito.get("duplicado", 0))
        self.assertEqual(sum(1 for r in registros if r["criado_em"] is None), gabarito.get("sem_data_hora", 0))
        ids = {p["id"] for p in CATALOGO}
        self.assertEqual(
            sum(1 for r in registros if r["lugar_id"] not in ids), gabarito.get("lugar_inexistente", 0)
        )


class OsmTest(unittest.TestCase):
    def test_reaproveita_coleta_recente_sem_consultar_a_rede(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta)
            origem = arquivos.caminho_coleta(osm.FONTE, "2026-11-12", base)
            arquivos.escrever_jsonl(origem, [{"type": "node", "id": 1}, {"type": "node", "id": 2}])
            resultado = osm.coletar("2026-11-13", base=base, max_idade_horas=12, endpoints=["http://invalido.test"])
            self.assertEqual(resultado["reaproveitado_de"], origem.name)
            self.assertEqual(resultado["registros"], 2)
            self.assertTrue(arquivos.caminho_coleta(osm.FONTE, "2026-11-13", base).exists())

    def test_sem_janela_nao_reaproveita(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta)
            arquivos.escrever_jsonl(arquivos.caminho_coleta(osm.FONTE, "2026-11-12", base), [{"id": 1}])
            self.assertIsNone(osm._coleta_recente(arquivos.caminho_coleta(osm.FONTE, "2026-11-13", base), 0))


class ArquivosTest(unittest.TestCase):
    def test_escrita_atomica_e_conferencia(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta)
            destino = arquivos.caminho_coleta("lugares", "2026-11-14", base)
            self.assertEqual(destino.name, "lugares_2026-11-14.jsonl")
            self.assertEqual(arquivos.escrever_jsonl(destino, [{"id": "á"}, {"id": "b"}]), 2)
            self.assertEqual(list(base.rglob("*.tmp")), [])
            self.assertEqual(json.loads(destino.read_text(encoding="utf-8").splitlines()[0]), {"id": "á"})
            resumo = arquivos.conferir("2026-11-14", ["lugares"], base)
            self.assertEqual(resumo["lugares"]["registros"], 2)

    def test_conferir_acusa_arquivo_ausente(self):
        with tempfile.TemporaryDirectory() as pasta:
            with self.assertRaises(FileNotFoundError):
                arquivos.conferir("2026-11-14", ["lugares"], Path(pasta))


if __name__ == "__main__":
    unittest.main()
