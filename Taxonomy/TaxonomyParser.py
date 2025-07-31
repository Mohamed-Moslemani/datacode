import pandas as pd
import re

class TaxonomyParser:
    def __init__(self):
        self.valid_prefix = "AN"
        self.valid_fields = {
            "BusinessUnit": {"Intl","Domestic"},
            "Objective": {"Booking","Brand", "Loyalty"},
            "Market": {"KSA", "UAE", "EGY","QAT"},
            "Language": {"AR", "EN"},
            "Audience": {"Expats","Locals","Business", "Leisure"},
            "Channel": {"Search","Display", "Social","Video"},
            "Month": {"Jan","Feb"},
            "Route": re.compile(r"^[A-Z]{3}-[A-Z]{3}$")
        }

    def parse_row(self, campaign):
        result = {
            "BusinessUnit": None,
            "Objective": None,
            "Market": None,
            "Language": None,
            "Audience": None,
            "Channel": None,
            "Month": None,
            "Route": None,
            "Validity": False
        }

        if not isinstance(campaign, str) or not campaign.startswith(self.valid_prefix + "_"):
            return result

        parts = campaign.split("_")
        if len(parts) != 9:
            return result

        _, bu,obj,market,lang,aud,ch, month,route= parts
        valid = (
            bu in self.valid_fields["BusinessUnit"]
            and obj in self.valid_fields["Objective"]
            and market in self.valid_fields["Market"]
            and lang in self.valid_fields["Language"]
            and aud in self.valid_fields["Audience"]
            and ch in self.valid_fields["Channel"]
            and month in self.valid_fields["Month"]
            and self.valid_fields["Route"].match(route) is not None
        )

        result.update({
            "BusinessUnit": bu,
            "Objective":obj,
            "Market":market,
            "Language":lang,
            "Audience": aud,
            "Channel":ch,
            "Month": month,
            "Route": route,
            "Validity": valid
        })
        return result

    def parse_dataframe(self,df,column="Campaign_Name"):
        parsed = df[column].apply(self.parse_row).apply(pd.Series)
        return pd.concat([df.reset_index(drop=True),parsed],axis=1)
