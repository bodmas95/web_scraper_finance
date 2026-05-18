from edgar import Company, set_identity
from src.crawler.base import BaseCrawler
from typing import Any, Dict, Optional
import pandas as pd


class EdgarCrawler(BaseCrawler):
    """
    EDGAR crawler using edgartools library.
    Implements row-level scale matching for accurate Currency/Unit assignment.
    """
    
    # Metadata columns to exclude from value cleaning
    METADATA_COLUMNS = {
        'concept', 'label', 'index', 'Currency', 'Unit', 'standard_concept',
        'level', 'abstract', 'dimension', 'is_breakdown', 'dimension_axis',
        'dimension_member', 'dimension_member_label', 'dimension_label',
        'balance', 'weight', 'preferred_sign', 'parent_concept', 'parent_abstract_concept'
    }
    
    def __init__(self, config):
        super().__init__({})
        self.config = config
        set_identity(config.identity)
    
    def fetch_filings(self, ticker: str, form: str):
        """Fetch company filings from SEC EDGAR."""
        company = Company(ticker)
        filings = company.get_filings(form=form)
        return list(filings)[:self.config.max_filings]
    
    def _clean_value(self, value, scale=''):
        """
        Clean and format numeric values with dynamic scale division.
        
        Args:
            value: Value to clean (numeric, string, or NaN)
            scale: Scale text ("Millions", "Thousands", "Billions", "Trillions", or "")
            
        Returns:
            Formatted string with thousand separators and appropriate scaling
        """
        try:
            if pd.isna(value) or value is None:
                return ''
            
            str_val = str(value).strip()
            if str_val in ['', 'nan', 'None', 'NaN', 'none']:
                return ''
            
            # Parse numeric value - handle both string and numeric types
            if isinstance(value, (int, float)):
                num_val = float(value)
            else:
                clean_str = str_val.replace(',', '').replace('(', '-').replace(')', '').strip()
                num_val = float(clean_str)
            
            if num_val == 0:
                return '0'
            
            # Apply dynamic scale division
            is_negative = num_val < 0
            abs_val = abs(num_val)
            
            scale_factors = {
                'Trillions': 1_000_000_000_000,
                'Billions': 1_000_000_000,
                'Millions': 1_000_000,
                'Thousands': 1_000
            }
            
            if scale in scale_factors:
                abs_val = abs_val / scale_factors[scale]
            
            # Format as whole number with thousand separators
            whole_number = int(round(abs_val))
            formatted = f"{whole_number:,}"
            
            return f"-{formatted}" if is_negative else formatted
            
        except (ValueError, TypeError) as e:
            # If conversion fails, return original string without nan/None
            return str_val.replace('nan', '').replace('None', '')
        except Exception as e:
            # Catch-all: return string representation
            return str(value).replace('nan', '').replace('None', '')
    
    def _normalize_concept(self, concept):
        """
        Normalize concept identifier (colon → underscore).
        Financial statements use: us-gaap_Cash
        XBRL facts use: us-gaap:Cash
        """
        return concept.replace(':', '_') if concept else concept
    
    def _build_concept_to_scale_map(self, stmt_obj):
        """
        Build mapping from XBRL concept to scale/currency information.
        
        Returns:
            dict: {concept: {'currency': str, 'scale': str, 'decimals': int}}
        """
        concept_map = {}
        
        try:
            if not hasattr(stmt_obj, 'xbrl') or not hasattr(stmt_obj.xbrl, 'facts'):
                return concept_map
            
            facts_df = stmt_obj.xbrl.facts.to_dataframe()
            
            for _, fact in facts_df.iterrows():
                concept_original = fact.get('concept')
                decimals = fact.get('decimals')
                unit_ref = fact.get('unit_ref')
                
                if not concept_original:
                    continue
                
                concept = self._normalize_concept(concept_original)
                
                if concept in concept_map:
                    continue
                
                # Determine currency
                currency = 'USD' if unit_ref == 'usd' else str(unit_ref).upper() if unit_ref else 'USD'
                
                # Determine scale from decimals
                scale = ''
                if decimals is not None:
                    try:
                        dec_int = int(float(decimals))
                        scale_mapping = {
                            -12: 'Trillions',
                            -9: 'Billions',
                            -6: 'Millions',
                            -3: 'Thousands'
                        }
                        scale = scale_mapping.get(dec_int, '')
                    except:
                        scale = ''
                
                concept_map[concept] = {
                    'currency': currency,
                    'scale': scale,
                    'decimals': decimals
                }
            
            if len(concept_map) > 0:
                print(f"    Built concept map with {len(concept_map)} unique concepts")
            
        except Exception as e:
            print(f"    Error building concept map: {e}")
        
        return concept_map
    
    def _has_row_values(self, row):
        """Check if row has any non-empty numeric values."""
        # Check if this is an abstract/header row
        if row.get('abstract') == True or row.get('abstract') == 'true':
            return False
        
        for col in row.index:
            if col not in self.METADATA_COLUMNS:
                val = str(row[col]).strip()
                if val and val not in ['', 'nan', 'None', 'NaN']:
                    return True
        return False
    
    def _get_currency_and_scale(self, row, concept_map):
        """
        Get currency and scale for a row based on its concept.
        
        Returns:
            tuple: (currency, scale)
        """
        # Check if row has values (blank row handling)
        if not self._has_row_values(row):
            return '', ''
        
        # Normalize and lookup concept
        concept_original = row.get('concept')
        concept = self._normalize_concept(concept_original)
        
        if concept and concept in concept_map:
            return concept_map[concept]['currency'], concept_map[concept]['scale']
        
        # Fallback: Try to infer scale from the most common scale in the map
        if concept_map:
            # Get most common scale from concept map
            scales = [info['scale'] for info in concept_map.values() if info['scale']]
            if scales:
                from collections import Counter
                most_common_scale = Counter(scales).most_common(1)[0][0]
                return 'USD', most_common_scale
        
        return 'USD', 'Millions'  # Default fallback to Millions for USD values
    
    def _apply_row_level_scaling(self, df, concept_map):
        """
        Apply row-level currency and scale matching, then clean values.
        
        Args:
            df: DataFrame with financial statement data
            concept_map: Mapping of concept to scale/currency
            
        Returns:
            DataFrame with Currency, Unit columns and cleaned values
        """
        if 'concept' not in df.columns or not concept_map:
            return df
        
        # Apply currency and scale to each row
        df[['Currency', 'Unit']] = df.apply(
            lambda row: pd.Series(self._get_currency_and_scale(row, concept_map)),
            axis=1
        )
        
        # Clean values with row-specific scale
        for col in df.columns:
            if col not in self.METADATA_COLUMNS:
                # Apply cleaning to each cell with its row's scale
                def clean_cell(row):
                    val = row[col]
                    scale = row.get('Unit', '')
                    cleaned = self._clean_value(val, scale)
                    return cleaned
                
                df[col] = df.apply(clean_cell, axis=1)
        
        # Move Currency and Unit to appear AFTER year columns (financial values)
        # Identify year columns (typically formatted as YYYY-MM-DD)
        year_cols = []
        metadata_cols = []
        other_cols = []
        
        for col in df.columns:
            if col in ['Currency', 'Unit']:
                continue  # Skip these, we'll add them later
            elif col in self.METADATA_COLUMNS:
                metadata_cols.append(col)
            elif '-' in str(col) and len(str(col)) == 10:  # Date format: YYYY-MM-DD
                year_cols.append(col)
            else:
                other_cols.append(col)
        
        # Reorder: metadata columns, year columns, Currency, Unit, other columns
        cols = metadata_cols + year_cols + ['Currency', 'Unit'] + other_cols
        df = df[cols]
        
        # Log matching statistics
        matched = (df['Currency'] != '').sum()
        blank = ((df['Currency'] == '') & (df['Unit'] == '')).sum()
        unmatched = len(df) - matched - blank
        print(f"    Matched: {matched} rows, Blank: {blank} rows, Unmatched: {unmatched} rows")
        
        # Show unmatched concepts for debugging
        if unmatched > 0:
            unmatched_rows = df[(df['Currency'] != '') & (df['Unit'] == '')]
            if len(unmatched_rows) > 0:
                print(f"    Unmatched concepts (using fallback scale):")
                for idx, row in unmatched_rows.head(3).iterrows():
                    concept = row.get('concept', 'N/A')
                    label = row.get('label', 'N/A')
                    print(f"      - {concept}: {label}")
        
        # Debug: Show sample of cleaned values
        if len(df) > 0:
            sample_row = df[df['Currency'] != ''].head(1)
            if len(sample_row) > 0:
                for col in df.columns:
                    if col not in self.METADATA_COLUMNS and col not in ['Currency', 'Unit']:
                        val = sample_row[col].iloc[0]
                        if val and str(val).strip():
                            print(f"    Sample cleaned value in '{col}': {val}")
                            break
        
        return df
    
    def _process_statement(self, stmt_obj, stmt_type, parent_company_name):
        """
        Process a single financial statement with row-level scale matching.
        
        Returns:
            dict or None: Processed statement data
        """
        if stmt_obj is None:
            return None
        
        try:
            # Convert to DataFrame
            if hasattr(stmt_obj, "to_dataframe"):
                df = stmt_obj.to_dataframe()
            elif isinstance(stmt_obj, pd.DataFrame):
                df = stmt_obj
            else:
                return None
            
            if df is None or df.empty:
                return None
            
            df = df.reset_index(drop=False)
            
            # Filter to parent company only (exclude subsidiaries)
            if 'dimension_member' in df.columns:
                print(f"  [{stmt_type}] Filtering to parent company only")
                parent_mask = df['dimension_member'].isna() | (df['dimension_member'] == '')
                subsidiary_count = len(df[~parent_mask])
                df = df[parent_mask].copy()
                if subsidiary_count > 0:
                    print(f"    Excluded {subsidiary_count} subsidiary rows")
            else:
                print(f"  [{stmt_type}] No subsidiaries found")
            
            if df.empty:
                return None
            
            # Build concept map and apply row-level scaling
            concept_map = self._build_concept_to_scale_map(stmt_obj)
            df = self._apply_row_level_scaling(df, concept_map)
            
            print(f"    {parent_company_name}: {len(df)} rows processed")
            
            # Remove unwanted columns before returning
            columns_to_remove = [
                'abstract', 'dimension', 'is_breakdown', 'dimension_axis',
                'dimension_member', 'dimension_member_label', 'dimension_label',
                'balance', 'weight', 'preferred_sign'
            ]
            
            # Drop columns that exist in the dataframe
            df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
            
            return df.to_dict(orient="records")
            
        except Exception as e:
            print(f"Error processing {stmt_type}: {e}")
            return None
    
    def fetch_company_financials(self, ticker: str, year: int = None):
        """
        Fetch company financial statements with row-level scale matching.
        
        Args:
            ticker: Stock ticker symbol
            year: Fiscal year (optional)
            
        Returns:
            Dictionary with company info and processed financial statements
        """
        try:
            company = Company(ticker)
            financials = company.get_financials()
            
            if not financials:
                print(f"No financials available for {ticker}")
                return None
            
            # Get financial statements
            balance_sheet_obj = None
            income_statement_obj = None
            cash_flow_obj = None
            
            try:
                balance_sheet_obj = financials.balance_sheet()
            except Exception as e:
                print(f"Balance sheet not available: {e}")
            
            try:
                income_statement_obj = financials.income_statement()
            except Exception as e:
                print(f"Income statement not available: {e}")
            
            try:
                cash_flow_obj = financials.cashflow_statement()
            except Exception as e:
                print(f"Cash flow not available: {e}")
            
            print(f"\nProcessing financials for {ticker} ({company.name})...")
            
            # Process statements with row-level scale matching
            result = {
                'ticker': ticker,
                'cik': company.cik,
                'company_name': company.name,
                'financials': {
                    company.name: {
                        'balance_sheet': self._process_statement(balance_sheet_obj, 'balance_sheet', company.name),
                        'income_statement': self._process_statement(income_statement_obj, 'income_statement', company.name),
                        'cash_flow_statement': self._process_statement(cash_flow_obj, 'cash_flow_statement', company.name)
                    }
                }
            }
            
            print(f"\nData extraction complete for {ticker}")
            
            return result
            
        except Exception as e:
            print(f"Error fetching financials for {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return None
