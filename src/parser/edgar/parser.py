from datetime import datetime, timezone
from src.edgar_proxy_wrapper import setup_edgar_with_proxy
setup_edgar_with_proxy()

class EdgarParser:
    @staticmethod
    def parse_filings(ticker:str, form: str, filings: list):
        parsed_data = []

        for filing in filings:
            parsed_data.append(
                {
                    "ticker": ticker,
                    "form": form,
                    "company":getattr(filing, "company", None),
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
            # Get the financials block from crawler output
            financial_block = financials_data.get("financials", {})
            
            # The crawler nests data under company name, so we need to extract it
            # Structure: {'financials': {company_name: {'balance_sheet': [...], ...}}}
            if financial_block:
                # Get the first (and only) company key
                company_keys = list(financial_block.keys())
                if company_keys:
                    company_name = company_keys[0]
                    company_data = financial_block[company_name]
                    
                    # Extract the statements
                    parsed_data["financials"]["balance_sheet"] = company_data.get("balance_sheet")
                    parsed_data["financials"]["income_statement"] = company_data.get("income_statement")
                    parsed_data["financials"]["cash_flow_statement"] = company_data.get("cash_flow_statement")
                    
                    # Log what we found
                    if parsed_data["financials"]["balance_sheet"] is not None:
                        row_count = len(parsed_data["financials"]["balance_sheet"])
                        print(f" Balance sheet loaded: {row_count} rows")
                    if parsed_data["financials"]["income_statement"] is not None:
                        row_count = len(parsed_data["financials"]["income_statement"])
                        print(f" Income statement loaded: {row_count} rows")
                    if parsed_data["financials"]["cash_flow_statement"] is not None:
                        row_count = len(parsed_data["financials"]["cash_flow_statement"])
                        print(f" Cash flow statement loaded: {row_count} rows")
                else:
                    print(" No company data found in financials block")
            else:
                print(" No financials block found in data")
            
            return parsed_data
        except Exception as e:
            print(f"Error while preparing parsed data: {e}")
            import traceback
            traceback.print_exc()
            return None
