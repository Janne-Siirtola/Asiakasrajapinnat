import json
import numpy as np
import pandas as pd
from .Customer import Customer

class JSONBuilder:
    """
    A class to build a JSON object from a DataFrame.
    """
    
    def __init__(self, customer: Customer):
        self.decimals_map = customer.decimals_map

    def fmt_time(self, t):
        if pd.isna(t) or str(t).lower() == "nan":
            return None
        s = str(t)
        # ensure leading zero, e.g. “8:5” → “08:05”
        parts = s.split(":")
        return f"{int(parts[0]):02d}:{int(parts[1]):02d}"

    def format_row(self, row):
        parts = []
        for col, val in row.items():
            # key as JSON string
            key = json.dumps(col, ensure_ascii=False)
            if col in self.decimals_map and pd.notna(val):
                # numeric with fixed decimals
                fmt = f"{{:.{self.decimals_map[col]}f}}"
                num = fmt.format(val)
                parts.append(f"{key}:{num}")
            else:
                # dump everything else normally (strings, ints, None, etc.)
                parts.append(f"{key}:{json.dumps(val, ensure_ascii=False)}")
        return "{" + ",".join(parts) + "}"

    def build_json(self, df_final) -> str:
        # — format dates to ISO
        df_final["Pvm"] = (
            pd.to_datetime(df_final["Pvm"], dayfirst=True)
            .dt.strftime("%Y-%m-%d")
        )

        # — format times to HH:MM
        df_final["Kello"] = df_final["Kello"].apply(self.fmt_time)

        # — normalize times (fill NaN → null in JSON)
        df_final = df_final.replace({np.nan: None})
        
        s = "["
        rows = df_final.to_dict(orient="records")
        for i, row in enumerate(rows):
            s += self.format_row(row) + "\n"
            if i < len(rows) - 1:
                s += ","
        s += "]"
        
        return s
        
        # 3) Build JSON
        """ with open("output.json", "w", encoding="utf-8") as f:
            f.write("[")
            s = "["
            rows = df_final.to_dict(orient="records")
            for i, row in enumerate(rows):
                s += self.format_row(row) + "\n"
                f.write(self.format_row(row)+"\n")
                if i < len(rows) - 1:
                    s += ","
                    f.write(",")
            s += "]"
            f.write("]")
            
        with open ("soutput.json", "w", encoding="utf-8") as f:
            f.write(s) """
