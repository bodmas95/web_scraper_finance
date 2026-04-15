from datetime import datetime, timezone

class EdgarParser:
    @staticmethod
    def parse_filings(ticker: str, form: str, filings: list):
        parsed_data = []

        for filing in filings:
            parsed_data.append(
                {
                    "ticker": ticker,
                    "form": form,
                    "company": getattr(filing, "company", None),
                    "cik": getattr(filing, "cik", None),
                    "filing_date": str(getattr(filing, "filing_date", "")),
                    "accession_no": getattr(filing, "accession_no", None),
                }
            )

        return parsed_data

    @staticmethod
    def parse_financials(ticker: str, financials_data, year: int = None):
        """
        Parse financial statements from edgartools DataFrames into clean JSON format.

        Args:
            ticker: Stock ticker symbol
            financials_data: Dictionary with balance_sheet, income_statement, cash_flow DataFrames
            year: Fiscal year

        Returns:
            Dictionary with parsed financial data
        """
        if not financials_data:
            return None

        parsed_data = {
            "ticker": ticker,
            "fiscal_year": year,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "financials": {
                "balance_sheet": None,
                "income_statement": None,
                "cash_flow_statement": None
            }
        }

        try:
            # Parse Balance Sheet DataFrame
            financial_block = financials_data.get("financials", {})
            parsed_data["financials"]["balance_sheet"] = financial_block.get("balance_sheet")
            parsed_data["financials"]["income_statement"] = financial_block.get("income_statement")
            parsed_data["financials"]["cash_flow_statement"] = financial_block.get("cash_flow_statement")

            if parsed_data["financials"]["balance_sheet"] is not None:
                print(f"balance sheet loaded")
            if parsed_data["financials"]["income_statement"] is not None:
                print(f"income statement sheet loaded")
            if parsed_data["financials"]["cash_flow_statement"] is not None:
                print(f"cash flow loaded")
            return parsed_data
        except Exception as e:
            print(f"Error while preparing parsed data: {e}")