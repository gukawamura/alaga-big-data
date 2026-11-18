"""Coleta previsão do Open-Meteo para o Volume de chegada."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

URL = "https://api.open-meteo.com/v1/forecast"


def coletar_previsao(config: Path, chegada: Path) -> Path:
    bairros = json.loads((config / "bairros.json").read_text(encoding="utf-8"))
    coletado = datetime.now(timezone.utc)
    resposta = requests.get(URL, params={
        "latitude": ",".join(str(b["latitude"]) for b in bairros),
        "longitude": ",".join(str(b["longitude"]) for b in bairros),
        "hourly": "precipitation", "forecast_hours": 13, "timezone": "GMT",
    }, timeout=45)
    resposta.raise_for_status()
    lotes = resposta.json()
    if not isinstance(lotes, list) or len(lotes) != len(bairros):
        raise ValueError("O Open-Meteo não retornou previsão para todos os bairros")

    inicio = coletado.replace(minute=0, second=0, microsecond=0)
    fim = inicio + timedelta(hours=12)
    registros = []
    for bairro, lote in zip(bairros, lotes, strict=True):
        horas = lote["hourly"]
        antes = len(registros)
        for instante, chuva in zip(horas["time"], horas["precipitation"], strict=True):
            # A precipitação é a soma da hora anterior ao horário informado.
            hora = datetime.fromisoformat(instante).replace(tzinfo=timezone.utc) - timedelta(hours=1)
            if chuva is not None and inicio <= hora < fim:
                registros.append({
                    "bairro_id": bairro["bairro_id"], "data_hora": hora.isoformat(),
                    "precipitacao_mm": chuva, "coletado_em": coletado.isoformat(),
                })
        if len(registros) == antes:
            raise ValueError(f"Previsão vazia para {bairro['bairro_id']}")

    pasta = chegada / "previsao"
    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / f"previsao_{coletado:%Y-%m-%dT%H%M%S%fZ}.json"
    temporario = destino.with_suffix(".tmp")
    temporario.write_text("".join(json.dumps(linha) + "\n" for linha in registros), encoding="utf-8")
    temporario.replace(destino)
    print(f"Open-Meteo: {len(registros)} horas por bairro no total · {len(bairros)} bairros · {destino}")
    return destino


if __name__ == "__main__":
    raiz = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=raiz / "config")
    parser.add_argument("--chegada", type=Path, default=raiz / "chegada")
    args = parser.parse_args()
    coletar_previsao(args.config, args.chegada)
