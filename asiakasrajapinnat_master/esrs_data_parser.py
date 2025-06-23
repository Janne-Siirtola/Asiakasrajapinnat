"""ESRS E5-5 waste figures parser and model."""

from dataclasses import dataclass, fields
from typing import Tuple

import pandas as pd


@dataclass
class EsrsDataModel:
    """Container for ESRS waste figures."""

    recovery: float = 0.0
    disposal: float = 0.0
    preparation_for_reuse: float = 0.0
    recycling: float = 0.0
    other_recovery_operations: float = 0.0
    incineration: float = 0.0
    landfilling: float = 0.0
    other_disposal_operations: float = 0.0

    @property
    def non_recycled(self) -> float:
        return (
            self.other_recovery_operations
            + self.incineration
            + self.landfilling
            + self.other_disposal_operations
        )


class EsrsDataParser:
    """Compute ESRS E5-5 waste figures from a DataFrame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Materiaalihyotyaste"] = df["Materiaalihyotyaste"] / 100
        df["Energiahyotyaste"] = df["Energiahyotyaste"] / 100
        return df

    def _split_by_hazardous(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        mask = df["EWCkoodi"].astype(str).str.contains(
            "*", regex=False, na=False)
        return df.loc[~mask].copy(), df.loc[mask].copy()

    def _build_model(self, df: pd.DataFrame) -> EsrsDataModel:
        tuoteryhmat_loppukasittely = [
            "AS",  # Asbesti
            "KAA",  # Kaatopaikkajate (sis. lasikuitu)
            "VI"  # Villa
        ]

        mat = df["Materiaalihyotyaste"]
        ene = df["Energiahyotyaste"]
        paino = df["Paino"]
        tuoteryhma = df["Tuoteryhma"]

        recovery = (
            (mat + ene) * paino).where(~tuoteryhma.isin(tuoteryhmat_loppukasittely), 0)
        disposal = paino.where(tuoteryhma.isin(tuoteryhmat_loppukasittely), 0)

        model = EsrsDataModel()
        model.recovery = recovery.sum()
        model.disposal = disposal.sum()
        model.preparation_for_reuse = 0.0
        model.recycling = (mat * paino).sum()
        model.other_recovery_operations = (ene * paino).sum()
        model.incineration = 0.0
        mask_sum = mat + ene
        model.landfilling = disposal.where(
            (mask_sum == 0.0) | (mask_sum == 1.0), 0).sum()
        model.other_disposal_operations = disposal.where(
            (mask_sum > 0) & (mask_sum < 1), 0).sum()
        return model

    @staticmethod
    def build_json(dm_non_h: EsrsDataModel, dm_h: EsrsDataModel, reporting_period: str, unit: str = "tonnes") -> dict:
        def build_category(dm: EsrsDataModel) -> dict:
            total = dm.non_recycled or 1.0
            round_val = 3
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
                    "otherDisposalOperations": round(dm.other_disposal_operations, round_val),
                },
                "nonRecycled": {
                    "weight": round(dm.non_recycled, round_val),
                    "percentage": round(dm.non_recycled / (dm.recovery + dm.disposal) * 100, 2)
                    if (dm.recovery + dm.disposal) != 0
                    else 0,
                },
            }

        return {
            "reportingPeriod": reporting_period,
            "unit": unit,
            "wasteByHazardousness": {
                "nonHazardous": build_category(dm_non_h),
                "hazardous": build_category(dm_h),
            },
        }

    def get_reporting_period(self, df: pd.DataFrame) -> str:
        # parse your column once
        df['Pvm_dt'] = pd.to_datetime(
            df['Pvm'], dayfirst=True, format='%Y-%m-%d')

        # get the min/max
        oldest = df['Pvm_dt'].min()
        newest = df['Pvm_dt'].max()

        # strip off the time‑of‑day
        oldest_date = oldest.date()
        newest_date = newest.date()

        return f"{oldest_date} - {newest_date}"

    def parse(self) -> dict:
        df = self._preprocess(self.df)
        non_h_df, h_df = self._split_by_hazardous(df)
        dm_non_h = self._build_model(non_h_df)
        dm_h = self._build_model(h_df)
        reporting_period = self.get_reporting_period(df)
        return self.build_json(dm_non_h, dm_h, reporting_period)
