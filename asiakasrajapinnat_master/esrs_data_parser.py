import json
import logging
from dataclasses import dataclass, fields
from pathlib import Path
import numpy as np
import pandas as pd


@dataclass
class EsrsDataModel:
    recovery: float = 0.0
    disposal: float = 0.0
    preparation_for_reuse: float = 0.0
    recycling: float = 0.0
    other_recovery_operations: float = 0.0
    incineration: float = 0.0
    landfilling: float = 0.0
    other_disposal_operations: float = 0.0
    non_recycled: float = 0.0

    def __add__(self, other: "EsrsDataModel") -> "EsrsDataModel":
        # Sum each field of two models
        return EsrsDataModel(**{
            f.name: getattr(self, f.name) + getattr(other, f.name)
            for f in fields(self)
        })


class EsrsDataParser:
    def __init__(self, df: pd.DataFrame):
        self.data = df

    def parse(self, df_non_h: pd.DataFrame):
        # keep only rows where Paino is not 0
        df_non_h = df_non_h[df_non_h['Paino'] != 0]

        keep_columns = ['Tyyppi', 'Nimike', 'Paino',
                        'Materiaalihyotyaste', 'Energiahyotyaste', 'EWCkoodi']
        df_non_h = df_non_h[keep_columns]

        df_non_h['Materiaalihyotyaste'] = df_non_h['Materiaalihyotyaste'] / 100
        df_non_h['Energiahyotyaste'] = df_non_h['Energiahyotyaste'] / 100

        mask = df_non_h['EWCkoodi'].astype(str).str.contains(
            '*', regex=False, na=False)
        df_h = df_non_h.loc[mask].copy()
        df_non_h = df_non_h.loc[~mask].copy()

        loaded_h, loaded_non_h = self.load_esrs_json()
        datamodel_non_h = self.calculate_esrs(df_non_h)
        datamodel_h = self.calculate_esrs(df_h)

        json_data = self.build_esrs_json(
            dm_non_h=datamodel_non_h+loaded_non_h,
            dm_h=datamodel_h+loaded_h
        )

        return df_non_h, df_h, json_data

    def load_esrs_json(self):
        ROOT = Path(__file__).resolve().parents[1]
        path = ROOT / "local_tests" / "results" / "esrsasd_yit.json"
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        wb = data["wasteByHazardousness"]
        nh_section = wb["nonHazardous"]
        h_section = wb["hazardous"]

        def model_from_section(sec: dict) -> "EsrsDataModel":
            return EsrsDataModel(
                recovery=sec["totalWasteGenerated"]["recovery"],
                disposal=sec["totalWasteGenerated"]["disposal"],
                preparation_for_reuse=sec["recovery"]["preparationForReuse"],
                recycling=sec["recovery"]["recycling"],
                other_recovery_operations=sec["recovery"]["otherRecoveryOperations"],
                incineration=sec["disposal"]["incineration"],
                landfilling=sec["disposal"]["landfilling"],
                other_disposal_operations=sec["disposal"]["otherDisposalOperations"],
                non_recycled=sec["nonRecycled"]["weight"]
            )

        # Create model instances for non-hazardous and hazardous waste
        non_h = model_from_section(nh_section)
        h = model_from_section(h_section)

        return h, non_h

    def build_esrs_json(self, dm_non_h: EsrsDataModel, dm_h: EsrsDataModel, unit="tonnes"):
        """
        Rakentaa ESRS E5-5 materiaalivirrat ulos -osion JSON-rakenteen
        dm_non_h: EsrsDataModel ei-vaaralliselle jätteelle
        dm_h: EsrsDataModel vaaralliselle jätteelle
        reporting_period: dict, esim. {"startDate": "2024-01-01", "endDate": "2024-12-31"}
        methodology: dict kuvaamaan mittausmetodia ja oletuksia
        unit: yksikkö, esim. "tonnes"
        """
        def build_category(dm: EsrsDataModel):
            total = dm.non_recycled or 1.0
            round_val = 3

            def pct(val):
                return round(val / total * 100, 1)
            """ non_recycled_weight = max(dm.non_recycled - (
                dm.preparation_for_reuse + dm.recycling + dm.other_recovery_operations), 0) """
            return {
                "totalWasteGenerated": {
                    "recovery": round(dm.recovery, round_val),
                    "disposal": round(dm.disposal, round_val),
                },
                "recovery": {
                    "preparationForReuse": round(dm.preparation_for_reuse, round_val),
                    "recycling": round(dm.recycling, round_val),
                    "otherRecoveryOperations": round(dm.other_recovery_operations, round_val),
                },
                "disposal": {
                    "incineration": round(dm.incineration, round_val),
                    "landfilling": round(dm.landfilling, round_val),
                    "otherDisposalOperations": round(dm.other_disposal_operations, round_val)
                },
                "nonRecycled": {
                    "weight": round(dm.non_recycled, round_val),
                    "percentage": round(dm.non_recycled / (dm.recovery + dm.disposal) * 100, round_val) if (dm.recovery + dm.disposal) != 0 else 0
                }
            }

        structure = {
            "unit": unit,
            "wasteByHazardousness": {
                "nonHazardous": build_category(dm_non_h),
                "hazardous": build_category(dm_h)
            }
        }
        return structure

    def calculate_esrs(self, df: pd.DataFrame):
        datamap = EsrsDataModel()

        loppukasiteltavat_tyypit = [
            1408, 1907, 3250, 3281, 3313, 3505, 3506, 94007,
            314079, 314310, 314322, 501407, 503401, 522300
        ]
        mat_hyotyaste = df['Materiaalihyotyaste']
        ene_hyotyaste = df['Energiahyotyaste']
        paino = df['Paino']
        tyyppi = df['Tyyppi']

        df['recovery'] = (
            mat_hyotyaste * paino + ene_hyotyaste * paino
        ).where(~tyyppi.isin(loppukasiteltavat_tyypit), 0)
        datamap.recovery = df['recovery'].sum()

        df['disposal'] = (
            paino
        ).where(tyyppi.isin(loppukasiteltavat_tyypit), 0)

        datamap.disposal = df['disposal'].sum()

        total_waste_generated = datamap.recovery + datamap.disposal

        df['preparation_for_reuse'] = 0
        datamap.preparation_for_reuse = 0

        df['recycling'] = (
            mat_hyotyaste * paino
        )
        datamap.recycling = df['recycling'].sum()

        df['other_recovery_operations'] = (
            ene_hyotyaste * paino
        )
        datamap.other_recovery_operations = df['other_recovery_operations'].sum(
        )

        recovery_total = (
            datamap.preparation_for_reuse +
            datamap.recycling +
            datamap.other_recovery_operations
        )

        df['incineration'] = 0
        datamap.incineration = 0

        s = ene_hyotyaste + mat_hyotyaste

        df['landfilling'] = (
            df['disposal']
        ).where(s == 0, 0)
        datamap.landfilling = df['landfilling'].sum()

        df['other_disposal_operations'] = (
            df['disposal']
        ).where(
            (s > 0) &
            (s < 1),
            0
        )
        datamap.other_disposal_operations = df['other_disposal_operations'].sum(
        )

        df['disposal_total'] = (round(
            df['other_recovery_operations'] +
            df['incineration'] +
            df['landfilling'] +
            df['other_disposal_operations'], 3)
        )
        disposal_total = (
            datamap.other_recovery_operations +
            datamap.incineration +
            datamap.landfilling +
            datamap.other_disposal_operations
        )

        datamap.non_recycled = disposal_total + datamap.other_recovery_operations

        return datamap

    def data_to_csv(self, df: pd.DataFrame, encoding):
        return df.to_csv(index=False, encoding=encoding, sep=";", decimal=".")
