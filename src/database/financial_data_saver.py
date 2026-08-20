"""
Financial Data Saver

Saves extracted financial data to MongoDB collections.
"""

from datetime import datetime
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class FinancialDataSaver:
    """Saves extracted financial data to MongoDB."""
    
    def __init__(self, mongo_client):
        """
        Initialize the saver.
        
        Args:
            mongo_client: MongoDB client instance
        """
        self.client = mongo_client
        self.db = mongo_client['UAT_HR1']
        self.collection = self.db['financial_statements']
        
        # Create indexes
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for fast lookup."""
        try:
            # Compound index on company + year + statement type
            self.collection.create_index([
                ('company_name', 1),
                ('fiscal_year', 1),
                ('statement_type', 1)
            ], unique=True)
            
            # Index on company name
            self.collection.create_index('company_name')
            
            # Index on fiscal year
            self.collection.create_index('fiscal_year')
            
            logger.info(" Financial data indexes created")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
    
    def save_extraction_results(
        self,
        company_name: str,
        fiscal_year: int,
        extraction_results: Dict,
        pdf_filename: Optional[str] = None,
        metadata: Optional[Dict] = None
    ):
        """
        Save extraction results to MongoDB.
        
        Args:
            company_name: Company name
            fiscal_year: Fiscal year
            extraction_results: Dictionary with statement types as keys
            pdf_filename: Original PDF filename (optional)
            metadata: Additional metadata (optional)
        
        Returns:
            dict: Summary of saved documents
        """
        saved_count = 0
        updated_count = 0
        errors = []
        
        for statement_type, data in extraction_results.items():
            try:
                # Prepare document
                document = {
                    'company_name': company_name,
                    'fiscal_year': fiscal_year,
                    'statement_type': statement_type,
                    'data': data,
                    'pdf_filename': pdf_filename,
                    'extracted_at': datetime.utcnow(),
                    'metadata': metadata or {}
                }
                
                # Upsert (insert or update)
                result = self.collection.update_one(
                    {
                        'company_name': company_name,
                        'fiscal_year': fiscal_year,
                        'statement_type': statement_type
                    },
                    {'$set': document},
                    upsert=True
                )
                
                if result.upserted_id:
                    saved_count += 1
                    logger.info(f" Saved {statement_type} for {company_name} ({fiscal_year})")
                else:
                    updated_count += 1
                    logger.info(f" Updated {statement_type} for {company_name} ({fiscal_year})")
                
            except Exception as e:
                error_msg = f"Error saving {statement_type}: {e}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        summary = {
            'saved': saved_count,
            'updated': updated_count,
            'errors': errors,
            'total': len(extraction_results)
        }
        
        logger.info(f" Save summary: {saved_count} saved, {updated_count} updated, {len(errors)} errors")
        
        return summary
    
    def get_financial_data(
        self,
        company_name: str,
        fiscal_year: Optional[int] = None,
        statement_type: Optional[str] = None
    ):
        """
        Retrieve financial data from MongoDB.
        
        Args:
            company_name: Company name
            fiscal_year: Fiscal year (optional, returns all years if not specified)
            statement_type: Statement type (optional, returns all types if not specified)
        
        Returns:
            list: List of matching documents
        """
        query = {'company_name': company_name}
        
        if fiscal_year:
            query['fiscal_year'] = fiscal_year
        
        if statement_type:
            query['statement_type'] = statement_type
        
        results = list(self.collection.find(query))
        
        logger.info(f" Found {len(results)} documents for {company_name}")
        
        return results
    
    def delete_financial_data(
        self,
        company_name: str,
        fiscal_year: Optional[int] = None,
        statement_type: Optional[str] = None
    ):
        """
        Delete financial data from MongoDB.
        
        Args:
            company_name: Company name
            fiscal_year: Fiscal year (optional, deletes all years if not specified)
            statement_type: Statement type (optional, deletes all types if not specified)
        
        Returns:
            int: Number of documents deleted
        """
        query = {'company_name': company_name}
        
        if fiscal_year:
            query['fiscal_year'] = fiscal_year
        
        if statement_type:
            query['statement_type'] = statement_type
        
        result = self.collection.delete_many(query)
        
        logger.info(f" Deleted {result.deleted_count} documents for {company_name}")
        
        return result.deleted_count
    
    def get_stats(self):
        """
        Get statistics about saved financial data.
        
        Returns:
            dict: Statistics
        """
        total_documents = self.collection.count_documents({})
        
        # Get unique companies
        companies = self.collection.distinct('company_name')
        
        # Get unique years
        years = self.collection.distinct('fiscal_year')
        
        # Get statement type counts
        pipeline = [
            {'$group': {
                '_id': '$statement_type',
                'count': {'$sum': 1}
            }}
        ]
        statement_counts = {
            item['_id']: item['count']
            for item in self.collection.aggregate(pipeline)
        }
        
        stats = {
            'total_documents': total_documents,
            'unique_companies': len(companies),
            'companies': sorted(companies),
            'years': sorted(years),
            'statement_counts': statement_counts
        }
        
        return stats


def save_extraction_to_mongodb(
    company_name: str,
    fiscal_year: int,
    extraction_results: Dict,
    pdf_filename: Optional[str] = None
):
    """
    Convenience function to save extraction results to MongoDB.
    
    Args:
        company_name: Company name
        fiscal_year: Fiscal year
        extraction_results: Dictionary with statement types as keys
        pdf_filename: Original PDF filename (optional)
    
    Returns:
        dict: Summary of saved documents
    """
    from src.cache import get_mongo_client
    
    mongo_client = get_mongo_client()
    saver = FinancialDataSaver(mongo_client)
    
    return saver.save_extraction_results(
        company_name=company_name,
        fiscal_year=fiscal_year,
        extraction_results=extraction_results,
        pdf_filename=pdf_filename
    )
