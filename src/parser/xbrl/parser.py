"""
General XBRL parser for French IFRS company filings via filings.xbrl.org.

Works directly from the OIM xBRL-JSON format — no HTML download needed.
Applicable to any company with an LEI that files under French IFRS.

Public API
----------
fetch_filings(lei, api_base, headers=None)
    -> list of filing dicts (sorted newest first)

fetch_xbrl_facts(filing, api_base, headers=None)
    -> list of fact dicts with keys:
       concept_full, concept_short, period_type, period_start, period_end,
       fy_year, unit, value, decimals

build_statements(facts, fy_year=None)
    -> dict: {statement_type: pd.DataFrame}
       Columns: French Label | English Label | Concept | Value

build_consolidated(all_facts_by_fy)
    -> dict: {statement_type: pd.DataFrame}
       Columns: French Label | English Label | Concept | FY2024 | FY2023 | ...

generate_excel_bytes(statements_dict)
    -> bytes  (simple xlsx, one sheet per statement type)
"""

import io
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src import http_client

# ---------------------------------------------------------------------------
# IFRS Full concept → (French label, English label)
# Covers the most common concepts in French IFRS consolidated accounts.
# ---------------------------------------------------------------------------

IFRS_CONCEPT_LABELS: Dict[str, Tuple[str, str]] = {
    # ── Income Statement ────────────────────────────────────────────────────
    "Revenue":
        ("Chiffre d'affaires", "Revenue"),
    "RevenueFromContractsWithCustomers":
        ("Chiffre d'affaires", "Revenue from contracts with customers"),
    "OtherIncome":
        ("Autres produits", "Other income"),
    "OtherOperatingIncome":
        ("Autres produits opérationnels", "Other operating income"),
    "CostOfSales":
        ("Coût des ventes", "Cost of sales"),
    "GrossProfit":
        ("Marge brute", "Gross profit"),
    "DistributionCosts":
        ("Frais de distribution", "Distribution costs"),
    "AdministrativeExpense":
        ("Charges administratives", "Administrative expenses"),
    "SellingGeneralAndAdministrativeExpense":
        ("Frais commerciaux, généraux et administratifs", "SG&A expenses"),
    "OtherExpenseByFunction":
        ("Autres charges", "Other expenses"),
    "OtherExpenseByNature":
        ("Autres charges par nature", "Other expenses by nature"),
    "EmployeeBenefitsExpense":
        ("Charges de personnel", "Employee benefits expense"),
    "DepreciationAmortisationAndImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss":
        ("Dotations aux amortissements et dépréciations", "Depreciation, amortisation and impairment"),
    "DepreciationAndAmortisationExpense":
        ("Dotations aux amortissements", "Depreciation and amortisation"),
    "DepreciationRightOfUseAssets":
        ("Amortissement des droits d'utilisation", "Depreciation of right-of-use assets"),
    "ImpairmentLossRecognisedInProfitOrLoss":
        ("Perte de valeur reconnue en résultat", "Impairment loss"),
    "RawMaterialsAndConsumablesUsed":
        ("Achats consommés", "Raw materials and consumables used"),
    "OtherOperatingExpense":
        ("Autres charges opérationnelles", "Other operating expenses"),
    "EarningsBeforeInterestTaxesDepreciationAndAmortisation":
        ("EBITDA", "EBITDA"),
    "OperatingExpense":
        ("Charges opérationnelles", "Operating expenses"),
    "ProfitLossFromOperatingActivities":
        ("Résultat opérationnel", "Operating income"),
    "ProfitLossBeforeTax":
        ("Résultat avant impôt", "Profit before tax"),
    "FinanceCosts":
        ("Coût de l'endettement financier", "Finance costs"),
    "FinanceIncome":
        ("Produits financiers", "Finance income"),
    "FinanceIncomeExpense":
        ("Résultat financier", "Finance income (expense)"),
    "FinanceCostsRecognisedInProfitOrLoss":
        ("Charges financières", "Finance costs"),
    "FinanceIncomeRecognisedInProfitOrLoss":
        ("Produits de trésorerie et d'équivalents", "Finance income"),
    "GainsLossesOnExchangeDifferences":
        ("Écarts de change", "Exchange differences"),
    "InterestExpenseOnBorrowings":
        ("Intérêts sur emprunts", "Interest expense on borrowings"),
    "InterestExpenseOnLeaseLiabilities":
        ("Intérêts sur dettes locatives", "Interest expense on lease liabilities"),
    "ShareOfProfitLossOfAssociatesAccountedForUsingEquityMethod":
        ("Quote-part dans le résultat des entreprises associées", "Share of profit of associates"),
    "IncomeTaxExpenseContinuingOperations":
        ("Impôt sur le résultat", "Income tax expense"),
    "ProfitLoss":
        ("Résultat net", "Net income / (loss)"),
    "ProfitLossAttributableToOwnersOfParent":
        ("Résultat net — part du groupe", "Net income attributable to owners of parent"),
    "ProfitLossAttributableToNoncontrollingInterests":
        ("Résultat net — intérêts minoritaires", "Net income attributable to non-controlling interests"),
    "BasicEarningsLossPerShare":
        ("Résultat de base par action (en euros)", "Basic earnings per share (EUR)"),
    "DilutedEarningsLossPerShare":
        ("Résultat dilué par action (en euros)", "Diluted earnings per share (EUR)"),
    # OCI
    "OtherComprehensiveIncome":
        ("Autres éléments du résultat global", "Other comprehensive income"),
    "OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLossNetOfTax":
        ("Éléments recyclables en résultat", "Items that may be reclassified to profit or loss"),
    "OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLossNetOfTax":
        ("Éléments non recyclables en résultat", "Items that will not be reclassified to profit or loss"),
    "ComprehensiveIncome":
        ("Résultat global", "Total comprehensive income"),

    # ── Balance Sheet – Assets ───────────────────────────────────────────────
    "Assets":
        ("Total actif", "Total assets"),
    "NoncurrentAssets":
        ("Total actif non courant", "Total non-current assets"),
    "CurrentAssets":
        ("Total actif courant", "Total current assets"),
    "Goodwill":
        ("Goodwill", "Goodwill"),
    "IntangibleAssetsOtherThanGoodwill":
        ("Autres immobilisations incorporelles", "Other intangible assets"),
    "PropertyPlantAndEquipment":
        ("Immobilisations corporelles", "Property, plant and equipment"),
    "RightOfUseAssets":
        ("Droits d'utilisation relatifs aux contrats de location", "Right-of-use assets"),
    "InvestmentProperty":
        ("Immeubles de placement", "Investment property"),
    "InvestmentsAccountedForUsingEquityMethod":
        ("Participations comptabilisées par mise en équivalence", "Investments accounted for using equity method"),
    "DeferredTaxAssets":
        ("Impôts différés actifs", "Deferred tax assets"),
    "NoncurrentFinancialAssets":
        ("Actifs financiers non courants", "Non-current financial assets"),
    "OtherNoncurrentAssets":
        ("Autres actifs non courants", "Other non-current assets"),
    "OtherNoncurrentReceivables":
        ("Autres créances non courantes", "Other non-current receivables"),
    "Inventories":
        ("Stocks", "Inventories"),
    "TradeAndOtherCurrentReceivables":
        ("Clients et autres créances courantes", "Trade and other current receivables"),
    "TradeReceivables":
        ("Clients", "Trade receivables"),
    "CurrentTaxAssets":
        ("Actifs d'impôts courants", "Current tax assets"),
    "CurrentFinancialAssets":
        ("Actifs financiers courants", "Current financial assets"),
    "OtherCurrentAssets":
        ("Autres actifs courants", "Other current assets"),
    "CashAndCashEquivalents":
        ("Trésorerie et équivalents de trésorerie", "Cash and cash equivalents"),

    # ── Balance Sheet – Liabilities & Equity ────────────────────────────────
    "EquityAndLiabilities":
        ("Total passif et capitaux propres", "Total equity and liabilities"),
    "Equity":
        ("Capitaux propres", "Total equity"),
    "EquityAttributableToOwnersOfParent":
        ("Capitaux propres — part du groupe", "Equity attributable to owners of parent"),
    "NoncontrollingInterests":
        ("Intérêts non contrôlants (minoritaires)", "Non-controlling interests"),
    "IssuedCapital":
        ("Capital social", "Share capital"),
    "SharePremium":
        ("Primes d'émission", "Share premium"),
    "RetainedEarnings":
        ("Réserves et résultats non distribués", "Retained earnings"),
    "OtherReserves":
        ("Autres réserves", "Other reserves"),
    "ReserveOfSharebasedPayments":
        ("Réserves de paiements fondés sur des actions", "Share-based payment reserve"),
    "TreasuryShares":
        ("Actions propres", "Treasury shares"),
    "Liabilities":
        ("Total passif", "Total liabilities"),
    "NoncurrentLiabilities":
        ("Total passif non courant", "Total non-current liabilities"),
    "CurrentLiabilities":
        ("Total passif courant", "Total current liabilities"),
    "NoncurrentBorrowings":
        ("Dettes financières non courantes", "Non-current borrowings"),
    "NoncurrentLeaseLiabilities":
        ("Dettes locatives non courantes", "Non-current lease liabilities"),
    "DeferredTaxLiabilities":
        ("Impôts différés passifs", "Deferred tax liabilities"),
    "NoncurrentProvisions":
        ("Provisions non courantes", "Non-current provisions"),
    "OtherNoncurrentLiabilities":
        ("Autres passifs non courants", "Other non-current liabilities"),
    "NoncurrentFinancialLiabilities":
        ("Passifs financiers non courants", "Non-current financial liabilities"),
    "CurrentBorrowings":
        ("Dettes financières courantes", "Current borrowings"),
    "CurrentLeaseLiabilities":
        ("Dettes locatives courantes", "Current lease liabilities"),
    "CurrentProvisions":
        ("Provisions courantes", "Current provisions"),
    "TradeAndOtherCurrentPayables":
        ("Fournisseurs et autres dettes courantes", "Trade and other current payables"),
    "TradePayables":
        ("Fournisseurs", "Trade payables"),
    "CurrentTaxLiabilities":
        ("Passifs d'impôts courants", "Current tax liabilities"),
    "OtherCurrentLiabilities":
        ("Autres passifs courants", "Other current liabilities"),

    # ── Cash Flow ────────────────────────────────────────────────────────────
    "CashFlowsFromUsedInOperatingActivities":
        ("Flux de trésorerie liés à l'activité", "Cash flows from operating activities"),
    "CashFlowsFromUsedInOperations":
        ("Flux générés par les opérations", "Cash generated from operations"),
    "CashFlowsFromUsedInInvestingActivities":
        ("Flux de trésorerie liés aux opérations d'investissement", "Cash flows from investing activities"),
    "CashFlowsFromUsedInFinancingActivities":
        ("Flux de trésorerie liés aux opérations de financement", "Cash flows from financing activities"),
    "ProfitLossAdjustedForNoncashItems":
        ("Capacité d'autofinancement", "Profit adjusted for non-cash items"),
    "AdjustmentsForDepreciationAndAmortisationExpense":
        ("Dotations aux amortissements et dépréciations", "Adjustments for depreciation and amortisation"),
    "AdjustmentsForImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss":
        ("Ajustements pour pertes de valeur", "Adjustments for impairment loss"),
    "AdjustmentsForIncomeTaxExpense":
        ("Charge d'impôt", "Adjustments for income tax expense"),
    "AdjustmentsForFinanceCosts":
        ("Ajustements pour coût de l'endettement", "Adjustments for finance costs"),
    "AdjustmentsForSharebasedPayments":
        ("Paiements en actions", "Adjustments for share-based payments"),
    "AdjustmentsForGainsLossesOnDisposalOfNoncurrentAssets":
        ("Plus ou moins-values de cession", "Adjustments for gains/losses on disposal"),
    "IncreaseDecreaseInWorkingCapital":
        ("Variation du besoin en fonds de roulement", "Increase (decrease) in working capital"),
    "IncreaseDecreaseInTradeAndOtherPayables":
        ("Variation des fournisseurs et autres dettes", "Increase (decrease) in trade payables"),
    "IncreaseDecreaseInTradeAndOtherReceivables":
        ("Variation des clients et autres créances", "Decrease (increase) in trade receivables"),
    "IncreaseDecreaseInInventories":
        ("Variation des stocks", "Decrease (increase) in inventories"),
    "IncomeTaxesPaid":
        ("Impôt versé", "Income taxes paid"),
    "IncomeTaxesPaidRefundClassifiedAsOperatingActivities":
        ("Impôt versé (activité opérationnelle)", "Income taxes paid (operating activities)"),
    "InterestPaid":
        ("Intérêts financiers payés", "Interest paid"),
    "InterestPaidClassifiedAsFinancingActivities":
        ("Intérêts payés (financement)", "Interest paid (financing activities)"),
    "InterestPaidClassifiedAsOperatingActivities":
        ("Intérêts payés (exploitation)", "Interest paid (operating activities)"),
    "PurchaseOfPropertyPlantAndEquipment":
        ("Acquisition d'immobilisations corporelles", "Purchase of PP&E"),
    "PurchaseOfIntangibleAssets":
        ("Acquisition d'immobilisations incorporelles", "Purchase of intangible assets"),
    "PurchaseOfPropertyPlantAndEquipmentIntangibleAssetsAndOtherNoncurrentAssets":
        ("Décaissements liés aux acquisitions d'immobilisations", "Payments for PP&E and intangibles"),
    "ProceedsFromDisposalOfPropertyPlantAndEquipment":
        ("Produits de cession d'immobilisations corporelles", "Proceeds from disposal of PP&E"),
    "ProceedsFromDisposalOfNoncurrentAssets":
        ("Produits de cession d'actifs non courants", "Proceeds from disposal of non-current assets"),
    "AcquisitionOfSubsidiariesNetOfCashAcquired":
        ("Acquisitions de filiales, nettes de trésorerie", "Acquisitions of subsidiaries, net of cash"),
    "ProceedsFromSaleOfInterestsInAssociates":
        ("Cessions de participations dans des entreprises associées", "Proceeds from sale of associates"),
    "ProceedsFromBorrowingsClassifiedAsFinancingActivities":
        ("Augmentation des dettes financières", "Proceeds from borrowings"),
    "RepaymentsOfBorrowingsClassifiedAsFinancingActivities":
        ("Remboursement des dettes financières", "Repayments of borrowings"),
    "RepaymentsOfLeaseLiabilitiesClassifiedAsFinancingActivities":
        ("Remboursement des dettes locatives", "Repayments of lease liabilities"),
    "PaymentsForRepurchaseOfShares":
        ("Rachat d'actions propres", "Payments for repurchase of shares"),
    "ProceedsFromIssuingShares":
        ("Émission d'actions", "Proceeds from issuing shares"),
    "DividendsPaid":
        ("Dividendes versés", "Dividends paid"),
    "DividendsPaidClassifiedAsFinancingActivities":
        ("Dividendes versés (financement)", "Dividends paid (financing activities)"),
    "EffectOfExchangeRateChangesOnCashAndCashEquivalents":
        ("Incidence des variations des cours des devises", "Effect of exchange rate changes on cash"),
    "IncreaseDecreaseInCashAndCashEquivalents":
        ("Variation de la trésorerie", "Net change in cash and cash equivalents"),
    "CashAndCashEquivalentsAtBeginningOfPeriod":
        ("Trésorerie d'ouverture", "Cash at beginning of period"),
    "CashAndCashEquivalentsAtEndOfPeriod":
        ("Trésorerie de clôture", "Cash at end of period"),
}


# ---------------------------------------------------------------------------
# Statement classifier — maps concept short name → statement type
# ---------------------------------------------------------------------------

STATEMENT_MAP: Dict[str, str] = {
    # Income Statement
    "Revenue":                              "Income Statement",
    "RevenueFromContractsWithCustomers":    "Income Statement",
    "OtherIncome":                          "Income Statement",
    "OtherOperatingIncome":                 "Income Statement",
    "CostOfSales":                          "Income Statement",
    "GrossProfit":                          "Income Statement",
    "DistributionCosts":                    "Income Statement",
    "AdministrativeExpense":               "Income Statement",
    "SellingGeneralAndAdministrativeExpense": "Income Statement",
    "OtherExpenseByFunction":               "Income Statement",
    "OtherExpenseByNature":                 "Income Statement",
    "EmployeeBenefitsExpense":              "Income Statement",
    "DepreciationAmortisationAndImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss": "Income Statement",
    "DepreciationAndAmortisationExpense":   "Income Statement",
    "DepreciationRightOfUseAssets":         "Income Statement",
    "ImpairmentLossRecognisedInProfitOrLoss": "Income Statement",
    "RawMaterialsAndConsumablesUsed":       "Income Statement",
    "OtherOperatingExpense":                "Income Statement",
    "EarningsBeforeInterestTaxesDepreciationAndAmortisation": "Income Statement",
    "OperatingExpense":                     "Income Statement",
    "ProfitLossFromOperatingActivities":    "Income Statement",
    "FinanceCosts":                         "Income Statement",
    "FinanceIncome":                        "Income Statement",
    "FinanceIncomeExpense":                 "Income Statement",
    "FinanceCostsRecognisedInProfitOrLoss": "Income Statement",
    "FinanceIncomeRecognisedInProfitOrLoss": "Income Statement",
    "GainsLossesOnExchangeDifferences":     "Income Statement",
    "InterestExpenseOnBorrowings":          "Income Statement",
    "InterestExpenseOnLeaseLiabilities":    "Income Statement",
    "ShareOfProfitLossOfAssociatesAccountedForUsingEquityMethod": "Income Statement",
    "ProfitLossBeforeTax":                  "Income Statement",
    "IncomeTaxExpenseContinuingOperations": "Income Statement",
    "ProfitLoss":                           "Income Statement",
    "ProfitLossAttributableToOwnersOfParent": "Income Statement",
    "ProfitLossAttributableToNoncontrollingInterests": "Income Statement",
    "BasicEarningsLossPerShare":            "Income Statement",
    "DilutedEarningsLossPerShare":          "Income Statement",
    "OtherComprehensiveIncome":             "Income Statement",
    "OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLossNetOfTax": "Income Statement",
    "OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLossNetOfTax": "Income Statement",
    "ComprehensiveIncome":                  "Income Statement",
    # Assets
    "Assets":                               "Assets",
    "NoncurrentAssets":                     "Assets",
    "CurrentAssets":                        "Assets",
    "Goodwill":                             "Assets",
    "IntangibleAssetsOtherThanGoodwill":    "Assets",
    "PropertyPlantAndEquipment":            "Assets",
    "RightOfUseAssets":                     "Assets",
    "InvestmentProperty":                   "Assets",
    "InvestmentsAccountedForUsingEquityMethod": "Assets",
    "DeferredTaxAssets":                    "Assets",
    "NoncurrentFinancialAssets":            "Assets",
    "OtherNoncurrentAssets":                "Assets",
    "OtherNoncurrentReceivables":           "Assets",
    "Inventories":                          "Assets",
    "TradeAndOtherCurrentReceivables":      "Assets",
    "TradeReceivables":                     "Assets",
    "CurrentTaxAssets":                     "Assets",
    "CurrentFinancialAssets":               "Assets",
    "OtherCurrentAssets":                   "Assets",
    "CashAndCashEquivalents":               "Assets",
    # Liabilities & Equity
    "EquityAndLiabilities":                 "Liabilities",
    "Equity":                               "Liabilities",
    "EquityAttributableToOwnersOfParent":   "Liabilities",
    "NoncontrollingInterests":              "Liabilities",
    "IssuedCapital":                        "Liabilities",
    "SharePremium":                         "Liabilities",
    "RetainedEarnings":                     "Liabilities",
    "OtherReserves":                        "Liabilities",
    "ReserveOfSharebasedPayments":          "Liabilities",
    "TreasuryShares":                       "Liabilities",
    "Liabilities":                          "Liabilities",
    "NoncurrentLiabilities":               "Liabilities",
    "CurrentLiabilities":                   "Liabilities",
    "NoncurrentBorrowings":                 "Liabilities",
    "NoncurrentLeaseLiabilities":          "Liabilities",
    "DeferredTaxLiabilities":              "Liabilities",
    "NoncurrentProvisions":                "Liabilities",
    "OtherNoncurrentLiabilities":          "Liabilities",
    "NoncurrentFinancialLiabilities":      "Liabilities",
    "CurrentBorrowings":                   "Liabilities",
    "CurrentLeaseLiabilities":             "Liabilities",
    "CurrentProvisions":                   "Liabilities",
    "TradeAndOtherCurrentPayables":        "Liabilities",
    "TradePayables":                       "Liabilities",
    "CurrentTaxLiabilities":              "Liabilities",
    "OtherCurrentLiabilities":            "Liabilities",
    # Cash Flow
    "CashFlowsFromUsedInOperatingActivities":   "Cash Flow",
    "CashFlowsFromUsedInOperations":             "Cash Flow",
    "CashFlowsFromUsedInInvestingActivities":   "Cash Flow",
    "CashFlowsFromUsedInFinancingActivities":   "Cash Flow",
    "ProfitLossAdjustedForNoncashItems":        "Cash Flow",
    "AdjustmentsForDepreciationAndAmortisationExpense": "Cash Flow",
    "AdjustmentsForImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss": "Cash Flow",
    "AdjustmentsForIncomeTaxExpense":           "Cash Flow",
    "AdjustmentsForFinanceCosts":               "Cash Flow",
    "AdjustmentsForSharebasedPayments":         "Cash Flow",
    "AdjustmentsForGainsLossesOnDisposalOfNoncurrentAssets": "Cash Flow",
    "IncreaseDecreaseInWorkingCapital":         "Cash Flow",
    "IncreaseDecreaseInTradeAndOtherPayables":  "Cash Flow",
    "IncreaseDecreaseInTradeAndOtherReceivables": "Cash Flow",
    "IncreaseDecreaseInInventories":            "Cash Flow",
    "IncomeTaxesPaid":                          "Cash Flow",
    "IncomeTaxesPaidRefundClassifiedAsOperatingActivities": "Cash Flow",
    "InterestPaid":                             "Cash Flow",
    "InterestPaidClassifiedAsFinancingActivities": "Cash Flow",
    "InterestPaidClassifiedAsOperatingActivities": "Cash Flow",
    "PurchaseOfPropertyPlantAndEquipment":      "Cash Flow",
    "PurchaseOfIntangibleAssets":               "Cash Flow",
    "PurchaseOfPropertyPlantAndEquipmentIntangibleAssetsAndOtherNoncurrentAssets": "Cash Flow",
    "ProceedsFromDisposalOfPropertyPlantAndEquipment": "Cash Flow",
    "ProceedsFromDisposalOfNoncurrentAssets":   "Cash Flow",
    "AcquisitionOfSubsidiariesNetOfCashAcquired": "Cash Flow",
    "ProceedsFromSaleOfInterestsInAssociates":  "Cash Flow",
    "ProceedsFromBorrowingsClassifiedAsFinancingActivities": "Cash Flow",
    "RepaymentsOfBorrowingsClassifiedAsFinancingActivities": "Cash Flow",
    "RepaymentsOfLeaseLiabilitiesClassifiedAsFinancingActivities": "Cash Flow",
    "PaymentsForRepurchaseOfShares":            "Cash Flow",
    "ProceedsFromIssuingShares":                "Cash Flow",
    "DividendsPaid":                            "Cash Flow",
    "DividendsPaidClassifiedAsFinancingActivities": "Cash Flow",
    "EffectOfExchangeRateChangesOnCashAndCashEquivalents": "Cash Flow",
    "IncreaseDecreaseInCashAndCashEquivalents": "Cash Flow",
    "CashAndCashEquivalentsAtBeginningOfPeriod": "Cash Flow",
    "CashAndCashEquivalentsAtEndOfPeriod":       "Cash Flow",
}

# Preferred display order within each statement type
_STATEMENT_ORDER = {
    "Income Statement": [
        "Revenue", "RevenueFromContractsWithCustomers", "OtherIncome", "OtherOperatingIncome",
        "CostOfSales", "GrossProfit", "EmployeeBenefitsExpense", "RawMaterialsAndConsumablesUsed",
        "OtherOperatingExpense", "OtherExpenseByNature", "OperatingExpense",
        "DistributionCosts", "AdministrativeExpense", "SellingGeneralAndAdministrativeExpense",
        "OtherExpenseByFunction",
        "DepreciationAndAmortisationExpense", "DepreciationRightOfUseAssets",
        "DepreciationAmortisationAndImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss",
        "ImpairmentLossRecognisedInProfitOrLoss",
        "EarningsBeforeInterestTaxesDepreciationAndAmortisation",
        "ProfitLossFromOperatingActivities",
        "FinanceIncome", "FinanceIncomeRecognisedInProfitOrLoss",
        "FinanceCosts", "FinanceCostsRecognisedInProfitOrLoss",
        "InterestExpenseOnBorrowings", "InterestExpenseOnLeaseLiabilities",
        "GainsLossesOnExchangeDifferences", "FinanceIncomeExpense",
        "ShareOfProfitLossOfAssociatesAccountedForUsingEquityMethod",
        "ProfitLossBeforeTax", "IncomeTaxExpenseContinuingOperations",
        "ProfitLoss", "ProfitLossAttributableToOwnersOfParent",
        "ProfitLossAttributableToNoncontrollingInterests",
        "BasicEarningsLossPerShare", "DilutedEarningsLossPerShare",
        "OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLossNetOfTax",
        "OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLossNetOfTax",
        "OtherComprehensiveIncome", "ComprehensiveIncome",
    ],
    "Assets": [
        "Goodwill", "IntangibleAssetsOtherThanGoodwill", "PropertyPlantAndEquipment",
        "RightOfUseAssets", "InvestmentProperty",
        "InvestmentsAccountedForUsingEquityMethod", "NoncurrentFinancialAssets",
        "OtherNoncurrentReceivables", "DeferredTaxAssets", "OtherNoncurrentAssets",
        "NoncurrentAssets",
        "Inventories", "TradeReceivables", "TradeAndOtherCurrentReceivables",
        "CurrentTaxAssets", "CurrentFinancialAssets", "OtherCurrentAssets",
        "CashAndCashEquivalents", "CurrentAssets", "Assets",
    ],
    "Liabilities": [
        "IssuedCapital", "SharePremium", "RetainedEarnings", "OtherReserves",
        "ReserveOfSharebasedPayments", "TreasuryShares",
        "EquityAttributableToOwnersOfParent", "NoncontrollingInterests", "Equity",
        "NoncurrentBorrowings", "NoncurrentLeaseLiabilities", "NoncurrentFinancialLiabilities",
        "NoncurrentProvisions", "DeferredTaxLiabilities", "OtherNoncurrentLiabilities",
        "NoncurrentLiabilities",
        "CurrentBorrowings", "CurrentLeaseLiabilities", "TradePayables",
        "TradeAndOtherCurrentPayables", "CurrentTaxLiabilities",
        "CurrentProvisions", "OtherCurrentLiabilities", "CurrentLiabilities",
        "Liabilities", "EquityAndLiabilities",
    ],
    "Cash Flow": [
        "ProfitLoss", "ProfitLossAdjustedForNoncashItems",
        "AdjustmentsForDepreciationAndAmortisationExpense",
        "AdjustmentsForImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss",
        "AdjustmentsForSharebasedPayments", "AdjustmentsForFinanceCosts",
        "AdjustmentsForGainsLossesOnDisposalOfNoncurrentAssets",
        "AdjustmentsForIncomeTaxExpense",
        "IncreaseDecreaseInInventories", "IncreaseDecreaseInTradeAndOtherReceivables",
        "IncreaseDecreaseInTradeAndOtherPayables", "IncreaseDecreaseInWorkingCapital",
        "IncomeTaxesPaid", "IncomeTaxesPaidRefundClassifiedAsOperatingActivities",
        "InterestPaid", "InterestPaidClassifiedAsOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities", "CashFlowsFromUsedInOperations",
        "PurchaseOfPropertyPlantAndEquipment", "PurchaseOfIntangibleAssets",
        "PurchaseOfPropertyPlantAndEquipmentIntangibleAssetsAndOtherNoncurrentAssets",
        "ProceedsFromDisposalOfPropertyPlantAndEquipment",
        "ProceedsFromDisposalOfNoncurrentAssets",
        "AcquisitionOfSubsidiariesNetOfCashAcquired",
        "ProceedsFromSaleOfInterestsInAssociates",
        "CashFlowsFromUsedInInvestingActivities",
        "ProceedsFromBorrowingsClassifiedAsFinancingActivities",
        "RepaymentsOfBorrowingsClassifiedAsFinancingActivities",
        "RepaymentsOfLeaseLiabilitiesClassifiedAsFinancingActivities",
        "PaymentsForRepurchaseOfShares", "ProceedsFromIssuingShares",
        "DividendsPaid", "DividendsPaidClassifiedAsFinancingActivities",
        "InterestPaidClassifiedAsFinancingActivities",
        "CashFlowsFromUsedInFinancingActivities",
        "EffectOfExchangeRateChangesOnCashAndCashEquivalents",
        "IncreaseDecreaseInCashAndCashEquivalents",
        "CashAndCashEquivalentsAtBeginningOfPeriod", "CashAndCashEquivalentsAtEndOfPeriod",
    ],
}

STATEMENT_TYPES = ["Income Statement", "Assets", "Liabilities", "Cash Flow"]

# Default headers for the OIM API
_DEFAULT_HEADERS = {
    "Accept": "application/json,*/*",
    "User-Agent": "XBRL-Research/1.0 research@example.com",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _concept_short(concept_full: str) -> str:
    """'ifrs-full:Revenue' → 'Revenue'  (strip any namespace prefix)."""
    if ":" in concept_full:
        return concept_full.split(":")[-1]
    return concept_full


def _parse_period(period: str):
    """
    Parse an XBRL period string into (period_type, period_start, period_end, fy_year).

    Duration: '2022-01-01/2022-12-31' → ('duration', '2022-01-01', '2022-12-31', '2022')
    Instant:  '2022-12-31'            → ('instant',  '',           '2022-12-31', '2022')
    """
    if not period:
        return "unknown", "", "", ""
    if "/" in period:
        parts = period.split("/", 1)
        start, end = parts[0].strip(), parts[1].strip()
        fy_year = end[:4] if len(end) >= 4 else ""
        return "duration", start, end, fy_year
    else:
        fy_year = period[:4] if len(period) >= 4 else ""
        return "instant", "", period.strip(), fy_year


def _to_numeric(value) -> Optional[float]:
    """Convert a value to float; return None on failure."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_value(value: Optional[float], decimals) -> str:
    """Format a numeric value for display, applying decimals scaling."""
    if value is None:
        return ""
    try:
        dec = int(decimals)
        # decimals=-6 means value is in millions, -3 in thousands
        if dec <= -6:
            display = value / 1_000_000
            return f"{display:,.1f}"
        elif dec <= -3:
            display = value / 1_000
            return f"{display:,.1f}"
        else:
            return f"{value:,.0f}"
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_filings(lei: str, api_base: str, headers: dict = None) -> List[dict]:
    """
    Fetch filing list for a company by LEI from filings.xbrl.org API.
    Returns a list of filing dicts sorted by period_end descending.
    """
    hdrs = {**_DEFAULT_HEADERS, **(headers or {})}
    url = f"{api_base}/api/filings"
    resp = http_client.get(
        url,
        params={"filter[entity.identifier]": lei, "page[size]": 50},
        headers=hdrs,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    filings_raw = data.get("data", [])
    filings = []
    for f in filings_raw:
        attrs = dict(f.get("attributes", {}))
        attrs["_id"] = f.get("id", "")
        filings.append(attrs)
    filings.sort(key=lambda x: x.get("period_end", ""), reverse=True)
    return filings


def fetch_xbrl_facts(filing: dict, api_base: str, headers: dict = None) -> List[dict]:
    """
    Download the OIM xBRL-JSON for a filing and extract all numeric facts.

    Returns a list of fact dicts with keys:
        concept_full, concept_short, period_type, period_start, period_end,
        fy_year, unit, value_raw, value_numeric, decimals
    """
    json_url = filing.get("json_url", "")
    if not json_url:
        return []

    hdrs = {**_DEFAULT_HEADERS, **(headers or {})}
    full_url = api_base + json_url if not json_url.startswith("http") else json_url
    resp = http_client.get(full_url, headers=hdrs, timeout=120)
    resp.raise_for_status()

    try:
        data = resp.json()
    except Exception:
        data = json.loads(resp.content.decode("utf-8", errors="replace"))

    facts_raw = data.get("facts", {})
    result = []

    for _fact_id, fact in facts_raw.items():
        dims    = fact.get("dimensions", {})
        concept = dims.get("concept", "")
        period  = dims.get("period", "")
        unit    = dims.get("unit", "")
        raw_val = fact.get("value", "")
        decimals = fact.get("decimals", "")

        if not concept:
            continue

        p_type, p_start, p_end, fy_year = _parse_period(period)
        numeric = _to_numeric(raw_val)

        result.append({
            "concept_full":    concept,
            "concept_short":   _concept_short(concept),
            "period_type":     p_type,
            "period_start":    p_start,
            "period_end":      p_end,
            "fy_year":         fy_year,
            "unit":            unit,
            "value_raw":       raw_val,
            "value_numeric":   numeric,
            "decimals":        decimals,
        })

    return result


def _select_best_fact(facts_for_concept: List[dict]) -> Optional[dict]:
    """
    Among multiple facts for the same concept + period, pick the best one.
    Preference: EUR unit > numeric > first.
    """
    if not facts_for_concept:
        return None
    # Prefer EUR
    eur = [f for f in facts_for_concept if "EUR" in str(f.get("unit", "")).upper()]
    candidates = eur if eur else facts_for_concept
    # Prefer numeric
    numeric = [f for f in candidates if f.get("value_numeric") is not None]
    return (numeric or candidates)[0]


def build_statements(facts: List[dict], fy_year: str = None) -> Dict[str, pd.DataFrame]:
    """
    Build financial statement DataFrames from a list of facts (single filing).

    If fy_year is provided, only facts for that year are included.
    Returns dict: {statement_type: DataFrame}
    Columns: French Label | English Label | Concept | Value
    """
    # Filter by fy_year
    if fy_year:
        facts = [f for f in facts if f.get("fy_year") == fy_year]

    # Determine expected period type per statement
    # P&L and Cash Flow = duration; Balance Sheet = instant
    _stmt_period = {
        "Income Statement": "duration",
        "Cash Flow":        "duration",
        "Assets":           "instant",
        "Liabilities":      "instant",
    }

    # Group facts by concept_short + period_type, keeping best fact
    # key: (concept_short, period_type, fy_year)
    grouped: Dict[tuple, List[dict]] = {}
    for fact in facts:
        key = (fact["concept_short"], fact["period_type"], fact.get("fy_year", ""))
        grouped.setdefault(key, []).append(fact)

    best_facts: Dict[tuple, dict] = {k: _select_best_fact(v) for k, v in grouped.items()}

    result = {}

    for stmt_type in STATEMENT_TYPES:
        order = _STATEMENT_ORDER[stmt_type]
        expected_ptype = _stmt_period[stmt_type]
        rows = []

        for concept_short in order:
            stmt_for_concept = STATEMENT_MAP.get(concept_short)
            if stmt_for_concept != stmt_type:
                continue

            # Find best fact for this concept
            # Try expected period type first, then any
            fact = None
            for ptype in (expected_ptype, "duration", "instant"):
                key = (concept_short, ptype, fy_year or "")
                if key in best_facts and best_facts[key]:
                    fact = best_facts[key]
                    break
            # If fy_year not specified, search without fy_year filter
            if fact is None:
                for k, f in best_facts.items():
                    if k[0] == concept_short and f:
                        fact = f
                        break

            if fact is None:
                continue

            fr_label, en_label = IFRS_CONCEPT_LABELS.get(concept_short, (concept_short, concept_short))
            value_str = _format_value(fact.get("value_numeric"), fact.get("decimals", ""))

            rows.append({
                "French Label":   fr_label,
                "English Label":  en_label,
                "Concept":        concept_short,
                "Value":          value_str,
            })

        if rows:
            result[stmt_type] = pd.DataFrame(rows)

    return result


def build_consolidated(all_facts_by_fy: Dict[str, List[dict]]) -> Dict[str, pd.DataFrame]:
    """
    Build consolidated financial statement DataFrames with all years as columns.

    all_facts_by_fy: {fy_label: [facts_list]}  e.g. {'FY2024': [...], 'FY2023': [...]}

    Returns dict: {statement_type: DataFrame}
    Columns: French Label | English Label | Concept | FY2024 | FY2023 | ...
    """
    if not all_facts_by_fy:
        return {}

    fy_labels = sorted(all_facts_by_fy.keys(), reverse=True)

    # Build per-FY statement dicts
    per_fy: Dict[str, Dict[str, pd.DataFrame]] = {}
    for fy_label in fy_labels:
        fy_year = fy_label.replace("FY", "") if fy_label.startswith("FY") else fy_label
        per_fy[fy_label] = build_statements(all_facts_by_fy[fy_label], fy_year=fy_year)

    result = {}

    for stmt_type in STATEMENT_TYPES:
        order = _STATEMENT_ORDER[stmt_type]

        # Collect all data per concept across years
        concept_data: Dict[str, dict] = {}  # concept_short → row dict

        for concept_short in order:
            if STATEMENT_MAP.get(concept_short) != stmt_type:
                continue
            fr_label, en_label = IFRS_CONCEPT_LABELS.get(concept_short, (concept_short, concept_short))
            row = {
                "French Label":  fr_label,
                "English Label": en_label,
                "Concept":       concept_short,
            }
            has_any = False
            for fy_label in fy_labels:
                df = per_fy.get(fy_label, {}).get(stmt_type)
                if df is not None and not df.empty:
                    match = df[df["Concept"] == concept_short]
                    if not match.empty:
                        row[fy_label] = match.iloc[0]["Value"]
                        has_any = True
                    else:
                        row[fy_label] = ""
                else:
                    row[fy_label] = ""

            if has_any:
                concept_data[concept_short] = row

        if concept_data:
            columns = ["French Label", "English Label", "Concept"] + fy_labels
            df = pd.DataFrame(list(concept_data.values()), columns=columns)
            result[stmt_type] = df

    return result


def build_filing_view(facts: List[dict]) -> Dict[str, pd.DataFrame]:
    """
    Build financial statement DataFrames from a single filing's facts.

    A filing typically contains comparative data for two or more years.
    This function detects all years present and shows them as columns,
    so the user sees e.g. FY2024 | FY2023 side by side.

    Returns dict: {statement_type: DataFrame}
    Columns: French Label | English Label | Concept | FY2024 | FY2023 | ...
    """
    # Group facts by fy_year to create per-FY buckets
    all_facts_by_fy: Dict[str, List[dict]] = {}
    for fact in facts:
        fy_year = fact.get("fy_year", "")
        if fy_year:
            fy_lbl = f"FY{fy_year}"
        else:
            fy_lbl = "UNKNOWN"
        all_facts_by_fy.setdefault(fy_lbl, []).append(fact)

    return build_consolidated(all_facts_by_fy)


def generate_excel_bytes(statements_dict: Dict[str, pd.DataFrame]) -> bytes:
    """
    Generate a simple Excel workbook from statement DataFrames.
    One sheet per statement type.
    Returns raw bytes.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for stmt_type, df in statements_dict.items():
            if df is None or df.empty:
                continue
            # Clean sheet name (max 31 chars)
            sheet_name = stmt_type[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Auto-fit column widths
            ws = writer.sheets[sheet_name]
            for col_cells in ws.columns:
                max_len = 0
                col_letter = col_cells[0].column_letter
                for cell in col_cells:
                    try:
                        cell_len = len(str(cell.value)) if cell.value else 0
                        max_len = max(max_len, cell_len)
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(max_len + 4, 60)

    return output.getvalue()


def create_xbrl_facts_excel(all_facts: List[dict]) -> bytes:
    """
    Generate an Excel workbook with XBRL facts in two sheets:
      Sheet 1 – "All Facts":   one row per raw fact
      Sheet 2 – "By Concept":  pivoted (concept rows × year columns, values in thousands)

    Fact dicts are expected to have keys from fetch_xbrl_facts():
      concept_full, concept_short, period_type, period_start, period_end,
      fy_year, unit, value_numeric, decimals, fy_label
    """
    try:
        import xlsxwriter
    except ImportError:
        raise RuntimeError("xlsxwriter is required: pip install xlsxwriter")

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"nan_inf_to_errors": True})

    def F(**kw):
        d = {"font_name": "Arial", "font_size": 9, "valign": "vcenter"}
        d.update(kw)
        return wb.add_format(d)

    # ── Sheet 1: All Facts ────────────────────────────────────────────────────
    ws1 = wb.add_worksheet("All Facts")
    hdr = [
        "FY Label", "Concept (full)", "Namespace", "Concept (short)",
        "French Label", "English Label",
        "Period Type", "Period Start", "Period End", "FY Year",
        "Value (raw)", "Value (thousands)", "Unit", "Decimals",
    ]
    col_w = [10, 70, 14, 50, 50, 50, 10, 14, 14, 10, 18, 18, 30, 10]
    ws1.set_row(0, 20)
    for ci, (h, w) in enumerate(zip(hdr, col_w)):
        ws1.set_column(ci, ci, w)
        ws1.write(0, ci, h, F(bold=True, align="center", border=1))

    for ri, fact in enumerate(all_facts, start=1):
        concept_full  = fact.get("concept_full", "")
        concept_short = fact.get("concept_short", "")
        namespace     = concept_full.split(":")[0] if ":" in concept_full else ""
        fr_lbl, en_lbl = IFRS_CONCEPT_LABELS.get(concept_short, ("", ""))
        val_num = fact.get("value_numeric")
        try:
            val_thou = val_num / 1000 if val_num is not None else None
        except Exception:
            val_thou = None

        ws1.write(ri, 0,  fact.get("fy_label", ""),      F(border=1))
        ws1.write(ri, 1,  concept_full,                   F(border=1))
        ws1.write(ri, 2,  namespace,                      F(border=1))
        ws1.write(ri, 3,  concept_short,                  F(border=1))
        ws1.write(ri, 4,  fr_lbl,                         F(border=1))
        ws1.write(ri, 5,  en_lbl,                         F(border=1))
        ws1.write(ri, 6,  fact.get("period_type", ""),    F(border=1, align="center"))
        ws1.write(ri, 7,  fact.get("period_start", ""),   F(border=1, align="center"))
        ws1.write(ri, 8,  fact.get("period_end", ""),     F(border=1, align="center"))
        ws1.write(ri, 9,  fact.get("fy_year", ""),        F(border=1, align="center"))
        if val_num is not None:
            ws1.write_number(ri, 10, val_num,
                F(border=1, align="right", num_format="#,##0.##;(#,##0.##)"))
        else:
            ws1.write(ri, 10, fact.get("value_raw", ""), F(border=1))
        if val_thou is not None:
            ws1.write_number(ri, 11, val_thou,
                F(border=1, align="right", num_format="#,##0;(#,##0)"))
        else:
            ws1.write(ri, 11, "", F(border=1))
        ws1.write(ri, 12, fact.get("unit", ""),          F(border=1))
        ws1.write(ri, 13, str(fact.get("decimals", "")), F(border=1, align="center"))

    ws1.autofilter(0, 0, len(all_facts), len(hdr) - 1)
    ws1.freeze_panes(1, 0)

    # ── Sheet 2: By Concept (pivoted) ─────────────────────────────────────────
    ws2 = wb.add_worksheet("By Concept")
    all_years = sorted({f.get("fy_year") for f in all_facts if f.get("fy_year")})

    # Build pivot: (concept_full, concept_short, period_type) → {year: value_thousands}
    pivot: Dict[tuple, dict] = {}
    for fact in all_facts:
        if fact.get("value_numeric") is None:
            continue
        concept_full  = fact.get("concept_full", "")
        concept_short = fact.get("concept_short", "")
        period_type   = fact.get("period_type", "")
        key = (concept_full, concept_short, period_type)
        if key not in pivot:
            pivot[key] = {}
        yr = fact.get("fy_year")
        if yr and yr not in pivot[key]:
            try:
                pivot[key][yr] = fact["value_numeric"] / 1000
            except Exception:
                pass

    p_hdr = ["Concept (full)", "Concept (short)", "French Label", "English Label",
             "Period Type"] + [str(y) for y in all_years]
    p_w   = [70, 50, 50, 50, 10] + [16] * len(all_years)
    ws2.set_row(0, 20)
    for ci, (h, w) in enumerate(zip(p_hdr, p_w)):
        ws2.set_column(ci, ci, w)
        ws2.write(0, ci, h, F(bold=True, align="center", border=1))

    for ri, ((cf, cs, pt), yr_vals) in enumerate(pivot.items(), start=1):
        fr_lbl, en_lbl = IFRS_CONCEPT_LABELS.get(cs, ("", ""))
        ws2.write(ri, 0, cf, F(border=1))
        ws2.write(ri, 1, cs, F(border=1))
        ws2.write(ri, 2, fr_lbl, F(border=1))
        ws2.write(ri, 3, en_lbl, F(border=1))
        ws2.write(ri, 4, pt,     F(border=1, align="center"))
        for ci, yr in enumerate(all_years, start=5):
            val = yr_vals.get(yr)
            if val is not None:
                ws2.write_number(ri, ci, val,
                    F(border=1, align="right", num_format="#,##0;(#,##0)"))
            else:
                ws2.write(ri, ci, None, F(border=1))

    ws2.autofilter(0, 0, len(pivot), len(p_hdr) - 1)
    ws2.freeze_panes(1, 5)

    wb.close()
    output.seek(0)
    return output.getvalue()
