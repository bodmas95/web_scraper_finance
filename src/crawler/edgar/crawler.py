from edgar import Company, set_identity

from src.crawler.base import BaseCrawler
from src.crawler.proxy_base import ProxyBase
from typing import Any, Dict, Optional
import pandas as pd

class EdgarCrawler(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.proxy = ProxyBase(config.http_proxy, config.https_proxy)
        self.proxy.set_proxy()
        set_identity(config.identity)

    def fetch_filings(self, ticker: str, form: str):
        company = Company(ticker)
        filings = company.get_filings(form = form)
        return list(filings)[: self.config.max_filings]

    def _df_to_json(self, df_obj: Any):

        if df_obj is None:
            return None
        try:
            if hasattr(df_obj, "to_dataframe"):
                df = df_obj.to_dataframe()
            elif isinstance(df_obj, pd.DataFrame):
                df = df_obj
            else:
                return str(df_obj)
            if df is None or df.empty:
                return []
            df = df.reset_index(drop = False)
            return df.to_dict(orient = "records")
        except Exception as e:
            return {"error": f"Failed to convert statement into json: {e}"}

    def fetch_company_financials(self, ticker: str, year: int = None):
        """
        Fetch company financial statements using edgartools get_financials() method.

        Args:
            ticker: Stock ticker symbol
            year: Fiscal year (optional, gets latest if not specified)

        Returns:
            Dictionary with company info and financials DataFrames or None
        """
        try:
            company = Company(ticker)

            # Use the direct get_financials() method - much simpler!
            financials = company.get_financials()

            if not financials:
                print(f"No financials available for {ticker}")
                return None
            balance_sheet_obj = None
            income_statement_obj = None
            cash_flow_obj = None

            try:
                balance_sheet_obj = financials.balance_sheet()
            except Exception as e:
                print(f"Balance sheet not avaiable for {ticker}: {e}")

            try:
                income_statement_obj = financials.income_statement()
            except Exception as e:
                print(f"Income statement not avaiable for {ticker}: {e}")

            try:
                cash_flow_obj = financials.cashflow_statement()
            except Exception as e:
                print(f"Cash flow not avaiable for {ticker}: {e}")

            # Get the financial statements as DataFrames
            #balance_sheet = financials.balance_sheet()
            #income_statement = financials.income_statement()
            #cash_flow = financials.cashflow_statement()

            # Return a dictionary with all the data
            result = {
                'ticker': ticker,
                'cik': company.cik,
                'company_name': company.name,
                "financials": {
                    "balance_sheet": self._df_to_json(balance_sheet_obj),
                    "income_statement": self._df_to_json(income_statement_obj),
                    "cash_flow_statement": self._df_to_json(cash_flow_obj),
                }
                #'financials_obj': financials
            }
            print("Final Result before save=", result)
            return result
        except Exception as e:
            print(f"Error fetching financials for {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return None