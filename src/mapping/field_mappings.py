# ---------------------------------------------------------------------------
# BREF Field Mappings - Auto-generated from bref-validator.xlsx
# ---------------------------------------------------------------------------
# This file contains field definitions and their alias terms for AI mapping.
# Each field has a list of alternative names that the AI will search for
# in annual reports.
# ---------------------------------------------------------------------------

INCOME_STATEMENT_FIELDS = {
    "I30 | Sales (turnover)": 
     {
        "aliases": [
        "Revenue",
        "Revenue from operations",
        "Operating Revenue",
        "Total revenue",
        "Total Sales",
        "Turnover",
        "Net Sales",
        "Gross sales",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level":0
    },

    "I31 | Other operating revenue": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I79 | Less value-added and other taxes on sales and services": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I47 | Less sales returns & allowances": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "I1 | Total Net sales (turnover)": 
     {
        "aliases": [],
        "calculation": "I30+I31+I79+I47",
        "is_calculated": True,
        "indent_level": 0
    },

    "I130 | o/w Net rental Income": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "I2 | Costs and expenses (or \"COGS\")": 
     {
        "aliases": [
        "Cost of Sales",
        "Cost of Revenue",
        "Cost of Services",
        "Cost of Products Sold",
        "Direct Costs of Sales",
        "Manufacturing Costs",
        "Fuel",
        "Power",
        "Fuel, purchased power and interchange",
        "Purchased power",
        "Interchange",
        "Costs and expenses",
        "Total costs and expenses",
        "COGS",
        "Service Maintenance Expense"
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
 
     "I3 | Gross profit - TRR31": 
     {
        "aliases": [],
        "calculation": "I1-I2",
        "is_calculated": True,
        "indent_level": 0
    },

    "I4 | SG&A Expense": 
     {
        "aliases": [
        "Selling",
        "General and Administrative Expenses",
        "Operating Expenses",
        "General and Administrative Expenses (G&A)",
        "Selling Expenses",
        "Marketing and Administrative Expenses",
        "Corporate Expenses",
        "Commercial Expenses",
    ],
        "calculation": "I48+I49+I53",
        "is_calculated": True,
        "indent_level": 0
    },

    "I48 | o/w administratives expenses": 
     {
        "aliases": [
        "General and Administrative Expenses (G&A Expenses)",
        "Administrative Costs",
        "Operating Expenses",
        "Selling",
        "General & Administrative Expenses (SG&A Expenses)",
        "Corporate Expenses",
        "Overhead Costs",
        "General and Administrative"
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "I49 | o/w distribution costs, advertising & promotion": 
     {
        "aliases": [
        "Selling Expenses",
        "Logistics Costs",
        "Fulfillment Costs",
        "Delivery Expenses",
        "Shipping Costs",
        "Marketing and Distribution Costs",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "I53 | o/w salaries & related costs": {
        "aliases":  [
        "Personnel Costs",
        "Employee Compensation",
        "Wages and Salaries",
        "Staff Costs",
        "Employee Costs",
        "Payroll Expenses",
        "Salaries",
        "Wages and Benefits",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

        "I54 | Research and Development costs":  {
        "aliases": [
        "Research and Development Expenses",
        "R&D Expenses",
        "Development Costs",
        "Innovation Costs",
        "New Product Development Costs",
        "Technology Development Expenses",
        "Exploration and Development Cost",
    ] ,
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I46 | Taxes (other than income tax) & Insurance":  
     {
        "aliases": [
        "Property Taxes",
        "Local Taxes",
        "Business Taxes",
        "Excise Taxes",
        "Levies",
        "Other Statutory Taxes",
        "Insurance Premiums",
        "Insurance Costs",
        "Policy Costs",
        "Insurances",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I55 | Bad debt expenses": 
     {
        "aliases": [
        "Provision for Doubtful Accounts",
        "Allowance for Doubtful Accounts Expense",
        "Doubtful Accounts Expense",
        "Uncollectible Accounts Expense",
        "Impairment Losses on Receivables",
        "Write-offs of Receivables",
        "Credit Losses Expense",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I42 | +/-Changes in inventories and property development inventories": 
     {
        "aliases": [
        "Increase/Decrease in Inventories",
        "Inventory Movements",
        "Changes in Stock",
        "Adjustments for Inventory",
        "Changes in Property Development Stock",
        "Property Development Inventory Movements",
        "Work-in-Progress on Property Development",
        "Development Properties - Changes",
        "Alias",
        "Changes in Inventories and Development Properties",
        "Inventory and Property Stock Adjustments",
        "Movements in Operating Assets - Inventories and Properties",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I5 | Other revenues from current operations": 
     {
        "aliases": [
        "Other Operating Revenue",
        "Other Operating Income",
        "Gains & Disposals",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I6 | Other expenses from current operations": 
     {
        "aliases": [
        "Other Operating Expenses",
        "Miscellaneous Expenses",
        "General Expenses",
        "Operating Costs - Other",
        "Other operations and maintenance",
        "Ancillary Expenses",
        "Non-primary Operating Expenses",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    "I81 | EBITDA": 
     {
        "aliases": [],
        "calculation": "I3-I4-I54-I46-I55+I42+I5-I6",
        "is_calculated": True,
        "indent_level": 0
    },

    "I8 | Depreciation, depletion and amortization": 
     {
        "aliases": [
        "D&A",
        "Depreciation",
        "Amortisation",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    "I10 | Other amortization (net)": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I12 | EBIT": 
     {
        "aliases": [],
        "calculation": "I81-I8-I10",
        "is_calculated": True,
        "indent_level": 0
    },


        "I9 | Impairment of  Goodwill": 
     {
        "aliases": [
        "Goodwill Impairment Charge",
        "Impairment Loss - Goodwill",
        "Write-down of Goodwill",
        "Goodwill Impairment Expense",
        "Impairment of Intangible Assets - Goodwill",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I15 | Interest Income": 
    {
        "aliases": [
        "Financial Income",
        "Int on Bank Deposits",
        "Interest income",
        "Interest Revenue",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I14 | Interest Expenses":
    {
        "aliases":  [
        "Finance Costs",
        "Borrowing Costs",
        "Interest Expense on Debt",
        "Cost of Borrowing",
        "Finance Charges",
        "Interest Paid",
        "Interest Expense"
    ] ,
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I17 | Net total Interest": 
     {
        "aliases": [],
        "calculation": "(I9)+I15-I14",
        "is_calculated": True,
        "indent_level": 0
    },


    "I52 | +/- Gain (loss) on extinguishment of debt": 
    {
        "aliases": [
        "Gain (loss) on debt redemption",
        "Gain (loss) on early retirement of debt",
        "Gain (loss) from debt restructuring",
        "Gain (loss) on settlement of debt",
        "Debt extinguishment income (expense)",
        "Gain (loss) from financial liabilities extinguishment",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I64 | +/-Equity affiliates or Joint arrangements": 
    {
        "aliases": [
        "Equity in losses of equity method investees",
        "Income/Loss from Equity Affiliates/Associates",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I13 | +/-Gain (loss) on foreign exchange": 
    {
        "aliases": [
        "Foreign Exchange Gain (Loss)",
        "Foreign Currency Gain (Loss)",
        "Net Foreign Exchange Gain (Loss)",
        "Foreign Exchange Differences",
        "Unrealized/Realized Foreign Exchange Gain (Loss)",
        "Exchange Rate Differences",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I32 | +/- Other financial income and (expenses)": 
    {
        "aliases": [
        "Other Finance Income/(Costs)",
        "Miscellaneous Financial Gains/(Losses)",
        "Investment Gains/(Losses) - Other",
        "Other Investment Income/(Expenses)",
        "Net Financial Income/(Expense) - Other",
        "Gains/(Losses) from Financial Instruments",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I33 | Net annual prov. for  fin. assets & amortization of debt discount": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I34 | Financial Income  (loss)": 
     {
        "aliases": [],
        "calculation": "I17+I52+I64+I13+I32+I33",
        "is_calculated": True,
        "indent_level": 0
    },


    "I44 | Grants and assistance": 
    {
        "aliases": [
        "Government Grants",
        "Subsidies",
        "Grants Received",
        "Assistance Received",
        "Public Funding",
        "Non-repayable Grants",
        "Economic Development Grants",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I56 | +/-Restructuring costs or gains & similar items": 
    {
        "aliases": [
        "Restructuring Charges",
        "Restructuring Expenses",
        "Restructuring Gains/(Losses)",
        "Reorganization Costs/Gains",
        "Severance Costs",
        "Closure Costs",
        "Business Transformation Costs",
        "Asset Impairment & Restructuring Charges",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I57 | Disposals of intangibles, subsidiaries and affiliates":
    {
        "aliases":  [
        "Sale of Intangible Assets",
        "Gain (loss) on Sale of Intangible Assets",
        "Write-off of Intangible Assets",
        "Pour \"Disposals of subsidiaries",
        "Sale of Subsidiaries",
        "Gain (loss) on Disposal of Subsidiaries",
        "Divestitures of Subsidiaries",
        "Sale of Business Units",
        "Sale of Investments in Associates",
        "Gain (loss) on Sale of Investments in Affiliates",
        "Divestment of Associates",
        "Gain (loss) on Sale of Assets",
        "Discontinued Operations",
        "Divestitures and Other Asset Disposals",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I19 | Disposals of property, plant and equipment": 
    {
        "aliases": [
        "Sale of Property",
        "Plant and Equipment (PP&E)",
        "Gain (loss) on Sale of PP&E",
        "Proceeds from Sale of Fixed Assets",
        "Disposal of Fixed Assets",
        "Write-off of Property",
        "Plant and Equipment",
        "Sale of Assets",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I60 | Share options granted to directors and employees": 
    {
        "aliases": [
        "Employee Stock Options",
        "Director Stock Options",
        "Share-Based Compensation Expense",
        "Equity-Settled Share-Based Payments",
        "Stock Option Awards",
        "Long-Term Incentive Plans (LTIP)",
        "Executive Stock Options",
        "Awards of Stock Options to Management",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "I20 | +/-Non operating income (loss) or Extraordinary gain (loss)": 
    {
        "aliases": [
        "Other Income/(Expenses)",
        "Miscellaneous Income/(Expenses)",
        "Finance Income/(Costs)",
        "Investment Income/(Losses)",
        "Gain (loss) from Unusual Items",
        "Exceptional Items Gain (Loss)",
        "Discontinued Operations",
        "Significant One-time Events Gain (Loss)",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    "I21 | Income (loss) before income taxes": 
     {
        "aliases": [],
        "calculation": "I34+I44+I56+I57+I19+I60+I20",
        "is_calculated": True,
        "indent_level": 0
    },

    "I35 | Provision for income taxes (benefit)": 
    {
        "aliases": [
        "Income Tax Expense/(Benefit)",
        "Current Tax Expense/(Benefit)",
        "Deferred Tax Expense/(Benefit)",
        "Provision for Taxes on Income",
        "Corporate Income Tax Expense/(Benefit)",
        "Taxes on Income",
        "Taxation",
        "Income Tax Charge",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

        "I62 | o/w current": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "I63 | o/w deferred": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "I36 | +/-Non-operating Equity affiliates or Joint arrangements": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    
    "I37 | Income from continuing operations": 
     {
        "aliases": [],
        "calculation": "I21-I35+I36",
        "is_calculated": True,
        "indent_level": 0
    },

    "I38 | Net income/(loss) from discontinued operations": 
    {
        "aliases": [
        "Net gain (loss) on disposal of business operations",
        "Profit (loss) from operations held for sale",
        "Results of discontinued segments",
        "Gain (loss) from divested activities",
        "Net impact of discontinued operations",
        "Income (loss) from segments to be disposed of",
    ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "I24 | Net profit for the year": 
    {
        "aliases": [
        "Net profit",
        "Profit",
        "Profit After tax",
    ],
        "calculation": "I37+I38",
        "is_calculated": True,
        "indent_level": 0
    },

      "I23 | Income attributable to Non-controlling interests": 
    {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    }

}


# ---------------------------------------------------------------------------
# Balance Sheet Fields - Updated with Calculation Logic
# Structure: {"field_code | label": {"aliases": [...], "calculation": "formula", "is_calculated": bool,"indent_level": 0}}
# ---------------------------------------------------------------------------

BALANCE_SHEET_FIELDS = {
    "B150 | Cash and bank deposits": {
        "aliases": ['Cash & Cash Equivalents', 'Cash & Short Term Investments'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B148 | Provisions for cash and bank deposits": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B17 | Short Term investments (marketable securities)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B149 | Restricted cash": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "B18 | Net Cash and cash equivalents (incl. marketable securities & excl. overdrafts)": {
            "aliases": ["Net cash and bank deposits (excluding overdrafts)", "Cash and bank deposits (excluding overdrafts)", "Cash and cash equivalents (excluding bank overdrafts)", "Cash and cash equivalents", "Cash and bank balances", "Cash at bank and in hand", "Cash and short‑term deposits", "Cash and term deposits (short‑term)", "Cash and bank accounts", "Cash on hand and at bank", "Bank deposits (current, unrestricted)", "Cash balance (excluding overdrafts)", "Cash and restricted cash (if grouped)", "Cash equivalents (money market funds, etc.)"],
            "calculation": "B150+B148+B17+B149",
            "is_calculated": True,
        "indent_level": 0
        },

    "B46 | Available-for-sale (AFS)": {
        "aliases": ["Assets held for sale", "Assets classified as held for sale", "Non‑current assets held for sale", "Disposal group assets held for sale", "Assets of disposal groups classified as held for sale", "Assets of disposal group", "Held‑for‑sale assets", "Assets held‑for‑sale / available‑for‑sale", "Assets available for sale (legacy phrasing)", "Available‑for‑sale assets (non‑current)", "Assets held for sale and discontinued operations", "Assets associated with discontinued operations (held for sale)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B15 | Trade Receivables / debtors":
    {
        "aliases": ['Accounts Receivables'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B33 | Other debtors": 
    {
        "aliases": ['Other Receivables'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B34 | o/w discounted bills": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B35 | Provisions for debtors": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B32 | Customers & other debtors, less allowance for doubtful accounts": 
      {
        "aliases": [],
        "calculation": "B46+B15+B33+B35",
        "is_calculated": True,
        "indent_level": 0
    },

    "B14 | Inventories (net)": 
    {
        "aliases": ['Stock', 'Raw Materials'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B36 | o/w Raw materials and supplies": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B37 | o/w In-process goods": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B38 | o/w Finished and semi-finished products": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B39 | o/w obsolescence allowance": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B114 | o/w Advances and down-payments to suppliers": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B98 | Other current operating assets": {
        "aliases": ["Other current operating assets", "Other current assets (operating)", "Other current non‑financial assets", "Current operating assets – other", "Miscellaneous current operating assets", "Sundry current operating assets", "Other current receivables (operating)", "Other current non‑trade receivables", "Prepayments and other current assets", "Prepaid expenses (current) and other current assets", "Prepaid operating expenses (current)", "Other current operating receivables", "Other current operating prepayments", "Other current assets – non‑financial", "Other current assets (excluding cash and financial)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B101 | o/w Derivatives relating to operating activities": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B117 | o/w Prepaid expenses": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B41 | Current financial assets (incl. Derivatives) - Net": {
        "aliases": ["Current financial assets", "Short‑term financial assets", "Financial assets – current", "Financial assets – short‑term", "Current investments", "Short‑term investments", "Marketable securities – current", "Trading securities – current", "Financial instruments – current assets", "Derivative financial assets – current", "Derivative assets – current", "Hedging instruments – assets (current)", "Current loans and receivables (financial)", "Short‑term loans and receivables (financial)", "Current financial assets at fair value", "Current financial assets held for trading", "Current financial investments"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B103 | o/w Loans and other debtors": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B42 | o/w Other current assets (incl. derivatives & short term investment)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B106 | o/w Tax and employee-related debtors": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B107 | o/w Debtor from Group and shareholders": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B108 | o/w Prepaid expenses": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B43 | o/w Provisions for current financial assets": {
        "aliases": ["Provisions for current financial assets", "Provision for impairment of current financial assets", "Provision for expected credit losses – current financial assets", "Allowance for impairment of current financial assets", "Allowance for losses on current financial assets", "Loss allowance – current financial assets", "Provision for doubtful financial assets (current)", "Provision for impairment – current investments", "Provision for impairment – current loans and receivables", "Provision for impairment – current marketable securities", "Provision for impairment of short‑term financial assets", "Impairment reserve for current financial assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B40 | Deferred and refundable incomes taxes": {
        "aliases": ["Current tax asset to recover", "Current income tax receivable", "Income tax receivable – current", "Current tax receivables", "Tax receivables – current", "Taxes recoverable – current", "Recoverable income taxes – current", "Corporate income tax receivable (current)", "Current tax assets", "Current tax assets – income tax", "Current tax refunds receivable", "Income tax recoverable (current)", "Tax credit receivable (current, income tax)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "B19 | Total current assets": {
            "aliases": [],
            "calculation": "B18+B32+B14+B98+B41+B40",
            "is_calculated": True,
        "indent_level": 0
        },

    "B4 | Fixed assets": {
        "aliases": ["Property, plant and equipment", "PPE", "Tangible assets", "Tangible fixed assets", "Fixed assets – tangible", "Land, buildings, plant and equipment", "Plant and equipment", "Buildings and machinery", "Property and equipment", "Property, plant and machinery", "Net property, plant and equipment", "Property, plant and equipment, net", "Net tangible assets (if used specifically for PPE)", "Non‑current tangible assets", "Operating fixed assets", "Industrial fixed assets", "Right‑of‑use assets", "Right of use assets", "Right‑of‑use asset", "Right of use asset", "Lease right‑of‑use assets", "Lease assets – right‑of‑use", "Leased right‑of‑use assets", "Assets under lease (right‑of‑use)", "ROU assets", "ROU asset", "Use‑of‑asset rights (lease)", "Right‑of‑use property, plant and equipment", "Right‑of‑use PPE", "Right‑of‑use buildings", "Right‑of‑use land and buildings", "Right‑of‑use vehicles", "Right‑of‑use equipment", "Right‑of‑use assets – non‑current", "Lease assets recognized under IFRS 16", "Lease assets recognized under ASC 842"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

      "B5 | Amortization/Depreciation of fixed assets (\"impaired assets\")": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
     "B6 | Net fixed assets (property, plants and equipment, Net)": {
            "aliases": [],
            "calculation": "B4+B5",
            "is_calculated": True,
        "indent_level": 0
        },

    "B20 | o/w Property, plant and equipment Net (\"non impaired assets\")": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B21 | o/w new Property, Plant and equipment": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B115 | o/w Construction in progress": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B116 | o/w Long term advances given (purchase deposits)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B7 | Property under capital leases, net": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B22 | Investment properties, net": {
        "aliases": ["Investment properties", "Investment property", "Properties held for investment", "Real estate investments", "Property investments", "Properties held to earn rentals", "Properties held for capital appreciation", "Real estate held for investment", "Investment real estate", "Non‑current investment properties", "Investment properties, net", "Investment property, net book value"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B23 | o/w non impaired investment properties": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B24 | o/w impaired investment properties": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B26 | o/w new investment properties": {
        "aliases": ["Investment property"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B2 | Goodwill (\"net\")": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B3 | Other assets and Intangible - Net": {
        "aliases": ["Other intangible assets, net", "Intangible assets (excluding goodwill)", "Intangible assets, net", "Net intangible assets", "Intangible assets – other", "Other intangibles, net book value", "Intangible fixed assets (excluding goodwill)", "Intangible assets – software, licenses, patents, trademarks", "Software and other intangible assets", "Licences, patents, trademarks and similar rights", "Capitalised development costs (intangible assets)", "Intangible operating assets (excluding goodwill)", "Intangible non‑current assets (other)", "Other intangible non‑current assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B27 | o/w non impaired intangible assets": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B28 | o/w impaired intangible assets": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B29 | o/w new intangible assets": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B30 | o/w provisions for fixed assets (non financial)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B48 | Employee benefits": {
        "aliases": ["Employee benefits (assets)", "Employee benefit assets", "Net defined benefit asset", "Surplus of defined benefit plan", "Pension plan assets (net)", "Retirement benefit assets", "Asset for employee benefits", "Employee benefit plan surplus", "Reimbursement rights under employee benefit plans", "Non‑current employee benefit assets", "Long‑term employee benefit assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B47 | Non current financial assets (incl derivatives & hedging financial instruments)": 
    {
        "aliases": ["Other Financial Assets", "Derivative Assets","Non‑current financial assets", "Long‑term financial assets", "Financial assets – non‑current", "Financial assets – long‑term", "Non‑current investments", "Long‑term investments", "Non‑current loans and receivables (financial)", "Long‑term loans and receivables", "Non‑current marketable securities", "Long‑term marketable securities", "Non‑current financial investments", "Derivative financial assets – non‑current", "Derivative assets – non‑current", "Hedging instruments – assets (non‑current)", "Financial instruments – non‑current assets", "Financial assets at fair value (non‑current)", "Available‑for‑sale financial assets – non‑current (legacy term)", "Held‑to‑maturity investments – non‑current", "Other non‑current financial assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B50 | o/w Non-current assets held-for-sale": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "B8 | Interest in net assets of joint ventures": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B9 | Investments in subsidiaries or non-controlled affiliates": {
        "aliases": ["Equity affiliates (net)", "Investments in equity affiliates (net)", "Investments in associates (net)", "Investments in joint ventures (net)", "Investments in associates and joint ventures (net)", "Investments in equity‑accounted investees (net)", "Equity‑accounted investments (net)", "Non‑consolidated investments (equity method)", "Non‑consolidated affiliates – investments and loans (net)", "Investments in related companies (equity affiliates)", "Investments in associated companies (net)", "Investments in jointly controlled entities (net)", "Loans to equity affiliates (net)", "Loans to associates and joint ventures (net)", "Investments and loans to affiliates (net)", "Affiliated companies – investments and loans (net)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B10 | Other assets or investments": 
    {
        "aliases": ["Investments", "Other non-current assets","Other noncurrent assets", "net of allowance of $9 and $77, respectively","Other non‑current assets", "Non‑current assets – other", "Other long‑term assets", "Long‑term assets – other", "Miscellaneous non‑current assets", "Sundry non‑current assets", "Other non‑current operating assets", "Other non‑current receivables", "Other long‑term receivables", "Other long‑term prepayments", "Other long‑term deposits and guarantees (assets)", "Other non‑current deferred charges", "Other non‑current assets (net)", "Non‑current sundry assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "B45 | Non current deferred & refundable income taxes": {
        "aliases": ["Deferred tax assets and tax receivables", "Deferred tax assets and income tax receivables", "Deferred tax assets and recoverable taxes", "Deferred income tax assets and tax receivables", "Deferred tax assets and taxes recoverable", "Deferred tax assets", "Deferred income tax assets", "Tax receivables", "Income tax receivables", "Recoverable income taxes", "Recoverable tax assets", "Tax assets – deferred and current receivables", "Deferred tax and tax receivables", "Deferred tax assets and other tax assets", "Deferred tax assets and tax credits", "Deferred tax assets and recoverable VAT/GST (if grouped)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "B13 | Total Long term assets": {
            "aliases": [],
            "calculation": "B6+B7+B22+B2+B3+B48+B47+B8+B9+B10+B45",
            "is_calculated": True,
        "indent_level": 0
        },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "B1 | TOTAL ASSETS": {
            "aliases": [],
            "calculation": "B19+B13",
            "is_calculated": True,
        "indent_level": 0
        },
#### Liabiilities Section US ######
    "L22 | Short-term debt < 1 yr , including current maturities of LT debt": 
    {
        "aliases": ["Financial Debt", "Bank debt", "Bonds", "Lease Liabilities","Short‑term borrowings", "Short‑term debt", "Commercial paper", "Current borrowings", "Current financial liabilities – borrowings", "Bank overdrafts and short‑term borrowings", "Bank overdrafts (if not treated as cash)", "Short‑term bank loans", "Short‑term bank borrowings", "Current portion of long‑term debt", "Current portion of long‑term borrowings", "Current maturities of long‑term debt", "Current maturities of long‑term borrowings", "Current portion of loans payable", "Current portion of bank loans", "Current portion of bonds payable", "Current portion of notes payable", "Current installment of long‑term loans", "Current portion of debentures", "Current portion of term loans", "Short‑term portion of interest‑bearing debt", "Debt due within one year", "Borrowings due within 12 months", "Loans due within one year", "Current financial borrowings", "Current interest‑bearing liabilities", "Current financial obligations – borrowings", "Short‑term interest‑bearing loans and borrowings", "Bank overdraft", "Bank overdrafts", "Overdrafts with banks", "Bank overdraft facility used", "Overdrawn bank accounts", "Cash at bank – overdrawn", "Bank current account overdraft", "Overdrawn current accounts", "Short‑term bank overdraft", "Bank overdraft – current liability", "Overdrafts and short‑term borrowings (if combined)", "Bank overdraft balances (liabilities)", "Bank overdraft and similar facilities", "Current portion of long‑term debt", "Current portion of long‑term borrowings", "Current maturities of long‑term debt", "Current maturities of long‑term borrowings", "Current portion of loans payable (long‑term loans)", "Current portion of bank loans (long‑term)", "Current portion of bonds payable (long‑term bonds)", "Current portion of debentures (long‑term debentures)", "Current portion of notes payable (long‑term notes)", "Current portion of term loans", "Long‑term debt due within one year", "Long‑term borrowings due within one year", "Installments of long‑term debt due in the next year", "Next‑year maturities of long‑term loans", "Short‑term portion of long‑term borrowings", "Short‑term portion of long‑term debt", "Finance lease liabilities – current", "Capital lease obligations – current", "Capital lease liabilities – current", "Lease liabilities – current (finance leases)", "Current portion of finance lease liabilities", "Current portion of capital lease obligations", "Current portion of lease liabilities (finance/capital)", "Short‑term finance lease liabilities", "Short‑term capital lease obligations", "Current lease obligations (finance/capital)", "Lease payments due within one year – principal (finance leases)", "Current portion of lease debt (finance leases)", "Finance lease debt – current", "Capital lease debt – current", "Discounted bills (current)", "Discounted trade bills", "Discounted bills of exchange", "Discounted promissory notes", "Bills discounted with banks", "Bills of exchange discounted", "Discounted receivables (with recourse)", "Factored receivables with recourse (if labelled as discounted bills)", "Discounted trade notes", "Discounted customer bills", "Discounted bills payable (if treated as borrowing)", "Short‑term borrowings – discounted bills", "Bills discounting facility", "Trade bill discounting", "Plain vanilla bonds (current)", "Bonds payable – current portion", "Bonds – current portion", "Straight bonds (current portion)", "Non‑convertible bonds – current portion", "Non‑derivative bonds – current portion", "Standard bonds (current portion)", "Plain vanilla notes (current)", "Plain vanilla debentures (current)", "Current portion of bonds", "Current portion of debentures", "Current portion of notes payable (bond‑type)", "Short‑term bonds payable", "Short‑term debentures", "Short‑term notes (bond‑type)", "Bonds due within one year", "Debentures due within one year", "Notes due within one year (fixed‑rate / plain vanilla)", "Hybrid borrowings (current)", "Hybrid debt (current)", "Hybrid instruments (current liabilities)", "Convertible bonds (current portion)", "Convertible debt (current portion)", "Convertible notes (current portion)", "Convertible loan notes (current portion)", "Convertible securities (current liability portion)", "Convertible debentures (current portion)", "Hybrid capital instruments (current liabilities)", "Perpetual and other hybrid instruments (current portion, if classified as debt)", "Current portion of convertible bonds", "Current portion of convertible debt", "Current portion of hybrid borrowings", "Short‑term convertible debt", "Short‑term convertible bonds", "Short‑term hybrid debt instruments", "Subordinated debt (current)", "Subordinated borrowings (current)", "Subordinated loans (current)", "Subordinated liabilities (current)", "Short‑term subordinated debt", "Short‑term subordinated borrowings", "Subordinated notes (current)", "Subordinated bonds (current portion)", "Subordinated loan notes (current)", "Junior debt (current)", "Junior subordinated debt (current)", "Debt subordinated to other creditors (current)", "Subordinated financial liabilities – current", "Subordinated interest‑bearing liabilities – current"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L64 | o/w  subordinated debt":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L24 | o/w  WC revolver": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L65 | o/w  convertible bonds": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L66 | o/w  plain vanilla bonds": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L67 | o/w  discounted bills": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L68 | o/w  capital lease obligations": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L23 | o/w  current portion of LT debt": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L162 | o/w Bank overdraft": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L75 | Accrued and other current liabilities": 
    {
        "aliases": ['Accrued expenses', 'Other current liabilities', 'Derivative Liabilities', 'loans from shareholders/related entities',"Accrued and other current liabilities", "Other current liabilities and accruals", "Accrued liabilities", "Accrued expenses", "Accrued charges", "Accrued operating expenses", "Other current liabilities", "Miscellaneous current liabilities", "Sundry current liabilities", "Other payables and accrued expenses (if not strictly trade)", "Accruals and deferred income (current)", "Accrued payroll and related liabilities", "Accrued employee benefits (current portion)", "Accrued interest (current, non‑debt classification)", "Accrued taxes (non‑income tax, e.g., VAT, GST)", "Accrued utilities, rent, etc.", "Accrued costs and other current liabilities", "Other accrued liabilities", "Other short‑term liabilities", "Current accrued and other liabilities", "Current accrued liabilities"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L72 | Provisions for current liabilities":  {
        "aliases": ["Provisions for current liabilities", "Current provisions", "Short‑term provisions", "Provisions (current portion)", "Current portion of provisions", "Provisions for warranties (current)", "Provision for warranty obligations – current", "Provisions for restructuring (current portion)", "Restructuring provision – current", "Provisions for legal claims (current portion)", "Legal provision – current", "Provisions for onerous contracts (current portion)", "Onerous contract provision – current", "Provision for environmental liabilities (current portion)", "Provision for employee benefits (current portion)", "Current provisions and contingent liabilities (when grouped)", "Provisions for risks and charges – current", "Short‑term provisions for risks and charges", "Other current provisions"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L74 | o/w provisions for guarantees granted": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L73 | o/w provisions for specific risks": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L21 | Trade Accounts payable / Creditors": 
    {
        "aliases": ['Trade Creditors', 'Accounts Payables','Accounts payable',"Trade accounts payable", "Trade payables", "Accounts payable – trade", "Trade creditors", "Sundry creditors (trade)", "Suppliers' payables", "Payables to suppliers", "Trade and other payables (trade portion)", "Bills payable (trade)", "Notes payable to suppliers", "Trade accounts and notes payable", "Trade accounts payable and accrued trade expenses", "Trade payables and accrued expenses (where clearly trade‑related)", "Commercial payables", "Trade liabilities"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L116 | Other payables / creditors": {
        "aliases": ["Other payables", "Non‑trade payables", "Sundry creditors (non‑trade)", "Other creditors", "Other current payables", "Miscellaneous payables", "Payables to employees (non‑wage accruals)", "Payables to related parties (non‑trade)", "Payables to group companies (non‑trade)", "Payables to directors/shareholders (non‑trade)", "Payables for taxes (excluding income tax if separately disclosed)", "VAT/GST payables (when grouped in other payables)", "Social security and pension payables", "Statutory dues payable", "Other liabilities – current (non‑trade)", "Other operating payables (non‑trade)", "Other short‑term payables", "Other current obligations (non‑trade)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L176 | Advances & downpayments Billings in excess of costs and estimated earnings on uncompleted contracts": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L114 | Dividends and interest on capital payable": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L70 | Current income tax liabilities (tax payable + deferrred tax)": {
        "aliases": ["Current income tax liabilities", "Income taxes payable (current)", "Income tax payable", "Corporate income tax payable", "Current tax liabilities", "Current income tax payables", "Current tax payable", "Taxes on income payable (current)", "Provision for income tax (current)", "Taxation payable (income tax – current)", "Income tax due within one year", "Short‑term income tax liabilities", "Current liabilities – income tax", "Current income tax obligations"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L113 | Payroll and related charges": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L115 | Employees' postretirement benefits obligation û Pension and Health Care": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L26 | Others financial liabilities & FV of financial instruments": {
        "aliases": ["Other financial liabilities (current)", "Other current financial liabilities", "Other financial liabilities – short‑term", "Financial liabilities at fair value (current)", "Financial instruments at fair value (liabilities, current)", "Derivative financial liabilities (current)", "Derivative instruments – liabilities (current)", "Derivative liabilities – current", "Fair value of financial instruments (liabilities, current)", "Fair value of derivatives – current liabilities", "Financial liabilities measured at fair value (current)", "Other current financial obligations", "Other current interest‑bearing financial liabilities (non‑borrowings)", "Current liabilities – hedging instruments", "Liabilities arising from hedging instruments (current)", "Liabilities from financial guarantees (current)", "Contingent consideration (current, financial liability)", "Other current financial instruments (liabilities)", "Other short‑term financial liabilities", "Miscellaneous current financial liabilities"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L25 | Loan from shareholders or related parties": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L112 | Other taxes payable, other than income tax": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L69 | Liabilities classified as held for sale": {
        "aliases": ["Liabilities classified as held for sale", "Liabilities of disposal groups classified as held for sale", "Liabilities associated with assets held for sale", "Liabilities of assets held for sale", "Liabilities related to disposal group held for sale", "Liabilities held for sale", "Held‑for‑sale liabilities", "Liabilities of discontinued operations (held for sale)", "Liabilities associated with discontinued operations (held for sale)", "Liabilities of disposal group", "Disposal group liabilities – held for sale", "Non‑current liabilities classified as held for sale (if specified)", "Current liabilities classified as held for sale (if specified)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "L27 | Total Current liabilities": {
            "aliases": [],
            "calculation": "L22+L75+L72+L21+L116+L176++L114+L70+L113+L115+L26+L69",
            "is_calculated": True,
        "indent_level": 0
        },

    "L15 | Long-term debt, less current maturities": 
    {
        "aliases": ["Bank Debt", "Bonds", "Lease Liabilities","Long‑term borrowings", "Long‑term debt", "Non‑current borrowings", "Non‑current financial liabilities – borrowings", "Non‑current interest‑bearing liabilities", "Long‑term loans", "Long‑term bank loans", "Long‑term bank borrowings", "Long‑term loans payable", "Long‑term notes payable", "Long‑term bonds payable", "Bonds payable (non‑current)", "Debentures (non‑current)", "Loan notes (non‑current)", "Term loans (non‑current portion)", "Non‑current portion of borrowings (excluding current maturities)", "Non‑current portion of loans payable", "Non‑current portion of bank loans", "Non‑current portion of bonds / debentures", "Interest‑bearing borrowings – non‑current", "Debt due after one year", "Borrowings due after 12 months", "Loans due after one year", "Long‑term interest‑bearing debt", "Long‑term financial obligations", "Long‑term financing liabilities", "Finance lease liabilities – non‑current", "Capital lease obligations – non‑current", "Capital lease liabilities – non‑current", "Lease liabilities – non‑current (finance/capital)", "Long‑term finance lease liabilities", "Long‑term capital lease obligations", "Non‑current portion of finance lease liabilities", "Non‑current portion of capital lease obligations", "Non‑current portion of lease liabilities (finance/capital)", "Lease obligations due after one year – finance leases", "Long‑term lease debt (finance leases)", "Finance lease debt – non‑current", "Capital lease debt – non‑current", "Plain vanilla bonds (non‑current)", "Bonds payable – non‑current", "Bonds – non‑current", "Straight bonds (non‑current)", "Non‑convertible bonds (non‑current)", "Non‑derivative bonds (non‑current)", "Standard bonds (non‑current)", "Plain vanilla debentures (non‑current)", "Plain vanilla notes (non‑current)", "Long‑term bonds payable", "Long‑term debentures", "Long‑term notes payable (bond‑type)", "Bonds due after one year", "Debentures due after one year", "Notes due after one year (fixed‑rate, plain vanilla)", "Medium‑term notes (non‑current portion)", "Senior unsecured bonds (non‑current)", "Hybrid borrowings (non‑current)", "Hybrid debt (non‑current)", "Hybrid financial instruments (non‑current liabilities)", "Hybrid capital instruments (non‑current liabilities)", "Convertible bonds (non‑current)", "Convertible debt (non‑current)", "Convertible notes (non‑current)", "Convertible loan notes (non‑current)", "Convertible debentures (non‑current)", "Convertible securities (liability component, non‑current)", "Long‑term convertible bonds", "Long‑term convertible debt", "Long‑term hybrid borrowings", "Long‑term hybrid capital instruments (classified as debt)", "Perpetual notes (liability‑classified hybrids)", "Perpetual bonds (hybrid liabilities)", "Mezzanine debt instruments (if labelled hybrid borrowings)", "Subordinated debt (non‑current)", "Subordinated borrowings (non‑current)", "Subordinated loans (non‑current)", "Subordinated liabilities (non‑current)", "Long‑term subordinated debt", "Long‑term subordinated borrowings", "Subordinated notes (non‑current)", "Subordinated bonds (non‑current)", "Subordinated loan notes (non‑current)", "Junior debt (non‑current)", "Junior subordinated debt (non‑current)", "Perpetual subordinated debt (if classified as liability)", "Debt subordinated to other creditors (non‑current)", "Subordinated financial liabilities – non‑current", "Subordinated interest‑bearing liabilities – non‑current"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L39 | o/w  senior debt": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L9 | o/w  subordinated debt": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L10 | o/w  Term loan": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L11 | o/w  Revolver": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L55 | o/w  convertible bonds": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L56 | o/w  plain vanilla bonds": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L13 | o/w  (finance) leasing": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L12 | o/w  other LT debt": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L16 | Deferred income tax liabilities and non-current tax liabilities": {
        "aliases": ["Deferred income tax liabilities", "Deferred tax liabilities", "Deferred taxation liabilities", "Deferred income taxes (liabilities)", "Deferred income tax – non‑current", "Non‑current tax liabilities", "Non‑current income tax liabilities", "Long‑term tax liabilities", "Long‑term income tax liabilities", "Deferred tax – liabilities (non‑current)", "Deferred tax liabilities and non‑current tax liabilities", "Deferred income taxes and long‑term tax liabilities", "Deferred tax obligations", "Future income tax liabilities", "Provision for deferred tax (liabilities)", "Deferred taxation – non‑current liabilities"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L59 | Employees' postretirement benefits obligation û Pension and Health Care": {
        "aliases": ["Provisions for pensions and employee benefits", "Pension provisions", "Provision for pensions", "Pension liabilities", "Retirement benefit obligations", "Post‑employment benefit obligations", "Defined benefit obligation (DBO)", "Defined benefit pension liabilities", "Employee benefit obligations – non‑current", "Non‑current employee benefit obligations", "Long‑term employee benefit liabilities", "Long‑term employee benefit provisions", "Provisions for employee benefits – non‑current", "Provision for post‑employment benefits", "Provision for retirement benefits", "Provision for gratuity (non‑current portion)", "Provision for long‑service awards (non‑current)", "Provision for other long‑term employee benefits", "Pension and other employee benefit obligations", "Pension and other long‑term employee benefit provisions"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L17 | Other LT provisions for liabilities & charges": {
        "aliases": ["Other non‑current provisions", "Other long‑term provisions", "Non‑current provisions (other)", "Long‑term provisions (other)", "Provisions for risks and charges – non‑current", "Provisions for risks and charges – long‑term", "Provisions for restructuring (non‑current portion)", "Restructuring provision – non‑current", "Provisions for legal claims (non‑current portion)", "Legal provisions – non‑current", "Provisions for environmental liabilities (non‑current)", "Environmental provision – non‑current", "Provisions for onerous contracts (non‑current)", "Onerous contract provisions – non‑current", "Provisions for guarantees (non‑current)", "Guarantee provisions – non‑current", "Other long‑term risk provisions", "Other provisions for contingencies – non‑current", "Other provisions – long‑term"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L18 | Other LT liabilities": 
    {
        "aliases": ["Other non-current liabilities", "Derivative Liabilities", "Other Financial Liabilities", "Loans from shareholders/related parties','Other noncurrent liabilities","Other non‑current liabilities", "Other long‑term liabilities", "Non‑current liabilities – other", "Long‑term liabilities – other", "Miscellaneous non‑current liabilities", "Miscellaneous long‑term liabilities", "Other non‑current financial liabilities (non‑borrowings)", "Other long‑term financial liabilities", "Non‑current deferred income", "Deferred income – non‑current", "Long‑term deferred revenue", "Long‑term contract liabilities", "Non‑current lease liabilities (if not separately presented)", "Long‑term lease obligations (if grouped)", "Non‑current liabilities to related parties (non‑trade)", "Long‑term payables to related parties", "Long‑term liabilities – government grants (deferred)", "Long‑term liabilities – contingent consideration", "Other non‑current obligations", "Other long‑term obligations"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L2 | Common stock": 
    {
        "aliases": ['Common equity', 'Issued and paid-Up Shares','Common stock'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L33 | Preferred stok": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L37 | Repurchased stock (common + preferred)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L4 | Additional paid-in capital": 
    {
        "aliases": ['Additional Equity','Additional paid-in capital'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L46 | o/w on hybrid Debts": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "L34 | Reserves and others":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    } ,

    "L36 | Treasury stocks":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L47 | Impairment differences":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L3 | Retained earnings":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L35 | Net result / Accumulated other comprehensive income (loss)":
     {
        "aliases": ['Other comprehensive income loss','Accumulated other comprehensive loss'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L7 | Equity attributable to owners of the company":  {
        "aliases": [],
        "calculation": "L2+L33+L37+L4+L34+L36+L47+L3+L35",
        "is_calculated": True,
        "indent_level": 0
    },

    "L8 | Non redeemable, non-controlling interest":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L177 | Mandatorily redeemable securities & other mezzanine equity":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "L111 | TOTAL EQUITY":  {
        "aliases": [],
        "calculation": "L7+L8+L177",
        "is_calculated": True,
        "indent_level": 0
    },

    "L28 | TOTAL LIABILITIES AND EQUITIES":  {
        "aliases": [],
        "calculation": "L27+L15+L16+L59+L17+L18+L111",
        "is_calculated": True,
        "indent_level": 0
    },

    "DMLTC1 | Dette MLT > 1 an":  {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

}


# ==============================================================================
# Cash Flow Fields
# ==============================================================================

CASH_FLOW_FIELDS = {
    "ACF19 | Net income": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF20 | +/-Income linked to associated companies": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF21 | +/-Cancellation of dividends (received from /paid to) unconsolidated companies.": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF22 | +/-Depreciation Amortization and Depletion": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF23 | +/-Amortization of Intangible Assets": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF24 | +/-Gain (loss) on sale of assets": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF25 | +/-Cost of financial debt, net": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF26 | +/-Tax (inc. Deffered tax, tax credit)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF27 | +/-Stock-based compensation expense": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF34 | +/- Litigation, fines": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF28 | +/- Others": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF36 | Cash-flow before cash interests and change in WC": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF35 | Gross interests (cash)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF01a | Cash-flow before change in WC (FFO)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF29 | EBITDA (published)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF31 | Income tax (cash)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF30 | +/- Litigation, fines": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF32 | +/- Others": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF01b | Cash-flow before cash interests and change in WC": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF37 | Gross interests (cash)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF38 | Cash-flow before change in WC (FFO)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF36bis | Cash-flow before cash interests and change in WC": 
    {
        "aliases": ['Cash flow from operations before interest and taxes'],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF35bis | Gross interests (cash)": 
    {
        "aliases": ["Cash interest paid","Gross interest (cash)", "Gross interests (cash)", "Interest (cash basis)", "Interest (cash)", "Interest paid and received (gross)", "Interest paid (cash)", "Interest received (cash)", "Interest expense (cash paid)", "Interest income (cash received)", "Cash interest expense", "Cash interest income", "Cash interest flows", "Finance costs (cash)", "Finance income (cash)", "Cash finance costs", "Cash finance income", "Interest on borrowings paid (cash)", "Interest on loans paid (cash)", "Interest on bonds paid (cash)", "Interest on debt paid (cash)", "Interest on bank loans paid (cash)", "Interest received on deposits (cash)", "Interest received on investments (cash)", "Interest received on loans granted (cash)", "Net interest (cash basis)", "Interest and similar charges (cash)", "Interest and similar income (cash)", "Cash flows from interest (gross)", "Cash interest paid and received"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF01 | Cash-flow before change in WC (FFO)": 
    {
        "aliases": ['Funds from operations'],
        "calculation": "ACF36-ACF35",
        "is_calculated": True,
        "indent_level": 0
    },

    
    "ACF02 | +/- Change in WC": {
            "aliases": [],
            "calculation": "ACF39+ACF40+ACF41+ACF42+ACF43",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF39 | Decrease (increase) in inventories": {
        "aliases": ["(Increase) / decrease in inventory", "(Increase) / decrease in stock", "Change in inventories", "Movement in goods and materials", "Change in raw materials, work in progress and finished goods"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF40 | Decrease (increase) in trade receivables": {
        "aliases": ["(Increase) / decrease in trade receivables", "(Increase) / decrease in accounts receivable", "Movement in debtors", "Change in customers receivables", "Change in trade and other receivables", "(Increase) / decrease in bills receivable"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF41 | Increase (decrease) in trade payables": {
        "aliases": ["(Increase) / decrease in trade payables", "Change in accounts payable", "Movement in creditors", "Change in suppliers", "Change in trade and other payables", "(Increase) / decrease in bills payable"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF42 | Changes in other assets and liabilities": 
    {
        "aliases": ["Change in deferred taxes/liabilities", "other changes","Change in other receivables", "Change in non‑trade receivables", "Change in prepayments", "Change in advances", "Change in tax receivables (income tax, VAT, GST)", "Change in sundry debtors (non‑trade)","Change in other payables", "Change in accrued liabilities", "Change in accruals", "Change in non‑trade creditors", "Change in tax payables (income tax, VAT, GST)", "Change in employee benefit payables", "Change in sundry creditors"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF43 | Others": 
    {
        "aliases": ["Cash taxes", "cash tax payments","Income tax paid", "Corporate tax paid", "Taxes on income paid", "Taxation paid", "Payment of income taxes", "Net income tax paid (including refunds)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF03 | Operating cash flow": 
    {
        "aliases": ['Cash flow from operations'],
        "calculation": "ACF01+ACF02",
        "is_calculated": True,
        "indent_level": 0
    },

        "ACF04 | CAPEX": {
            "aliases": [],
            "calculation": "ACF44+ACF45+ACF46",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF44 | (Purchases) sales of property, plant and equipment": 
    {
        "aliases": ["Additions to tangible/intangible assets", "Capital Expenditure","Purchases and sales of property, plant and equipment", "Purchases and disposals of property, plant and equipment", "Purchases/(sales) of property, plant and equipment", "Acquisitions and disposals of property, plant and equipment", "Additions and disposals of property, plant and equipment", "Purchase and sale of fixed assets", "Purchases and sales of fixed assets", "Purchase/(sale) of fixed assets", "Acquisition and disposal of fixed assets", "Capital expenditure and disposals of PPE", "Investments in property, plant and equipment", "Investments in and sales of fixed assets", "Purchase/(disposal) of tangible fixed assets", "Movements in property, plant and equipment (purchases and disposals)", "Movements in fixed assets (purchases and sales)", "Capital work‑in‑progress (CWIP)", "Construction in progress (CIP)", "Assets under construction", "Work in progress – capital projects", "Projects under development", "Purchase of fixed assets", "Acquisition of property, plant and equipment", "Capital expenditure for PPE", "Additions to fixed assets", "Purchase of tangible fixed assets", "Acquisition of intangible assets", "Capitalization of development costs", "Purchase of software", "Purchase of licenses", "Purchase of patents, trademarks", "Investments in intangible assets", "Acquisition of investment property", "Purchase of rental property", "Purchase of real estate held for investment", "Investments in real estate"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF45 | Proceeds from sales of property, plant and equipment": 
    {
        "aliases": ["Asset Sales", "Sales of PPE", "Divestment","Proceeds from sale of property, plant and equipment", "Proceeds from sales of property, plant and equipment", "Proceeds from disposal of property, plant and equipment", "Cash proceeds from sale of property, plant and equipment", "Cash received from sale of property, plant and equipment", "Cash received from disposal of property, plant and equipment", "Sale of property, plant and equipment (proceeds)", "Sale of property, plant and equipment (cash inflow)", "Disposal of property, plant and equipment (proceeds)", "Disposal of property, plant and equipment (cash inflow)", "Proceeds from sale of fixed assets", "Proceeds from disposal of fixed assets", "Cash received on sale of fixed assets", "Cash received on disposal of fixed assets", "Proceeds from sale of tangible fixed assets", "Proceeds from disposal of tangible fixed assets", "Proceeds from sale of PPE", "PPE disposals – cash received", "Proceeds from asset disposals (PPE)", "Proceeds from sale of property and equipment", "Sale of fixed assets", "Disposal of property, plant and equipment", "Proceeds from sale of tangible assets", "Proceeds from disposal of PPE", "Sale of investment property", "Disposal of investment property", "Proceeds from sale of real estate investments", "Sale of intangible assets", "Disposal of intangible assets", "Proceeds from sale of software / licenses", "Proceeds from disposal of intangibles"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF46 | +/- Others": 
    {
        "aliases": ['Gain/loss from sales in subsidiaries', 'other assets', 'investments etc.',"Other investing cash flows", "Other cash flows from investing activities", "Other cash movements from investing activities", "Other investing activities", "Other investment‑related cash flows", "Miscellaneous investing cash flows", "Miscellaneous investing activities", "Other investment cash movements", "Other investing inflows/(outflows)", "Other investing items", "Other investing transactions", "Other cash flows – investing section", "Other investing cash inflows/(outflows)", "Other investing operations", "Other investment operations", "Proceeds from sale of investments", "Redemption of investments", "Sale of marketable securities", "Proceeds from disposal of equity investments", "Proceeds from disposal of debt securities", "Purchase of investments", "Acquisition of equity investments", "Acquisition of debt securities", "Purchase of marketable securities", "Purchase of long‑term investments", "Purchase of short‑term investments", "Investment in bonds", "Purchase of financial assets"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "ACF05 | Recurring Free Cash-Flow": {
            "aliases": [],
            "calculation": "ACF03+ACF04",
            "is_calculated": True,
        "indent_level": 0
        },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "ACF06 | +/- Acquisitions net of disposals": {
            "aliases": [],
            "calculation": "ACF15+ACF16",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF15 | Acquisitions": 
    {
        "aliases": ["Acquisition of subsidiaries", "Purchase of stake/shares in Subsidiaries/Other Entities","Acquisitions", "Acquisition of subsidiaries", "Acquisition of subsidiary", "Acquisition of businesses", "Acquisition of business units", "Acquisition of operations", "Business combinations – cash consideration", "Purchase of subsidiaries", "Purchase of subsidiary undertakings", "Purchase of business operations", "Investment in subsidiaries (cash paid)", "Investment in associates and joint ventures (cash paid)", "Acquisition of affiliates", "Acquisition of equity interests in subsidiaries/affiliates", "Cash paid for acquisitions", "Cash consideration for acquisitions", "Cash paid for business combinations", "Acquisitions (net of cash acquired)", "Net cash outflow on acquisition of subsidiaries", "Net cash outflow on acquisition of businesses"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF16 | Disposals": 
    {
        "aliases": ["Divestment of stake/shares in Subsidiaries/Other Entities","Disposals", "Disposal of subsidiaries", "Disposal of subsidiary", "Disposal of businesses", "Disposal of business units", "Disposal of operations", "Sale of subsidiaries", "Sale of subsidiary undertakings", "Sale of business operations", "Disposal of affiliates", "Disposal of equity interests in subsidiaries/affiliates", "Cash received from disposals", "Cash proceeds from disposals", "Cash received from sale of subsidiaries", "Cash received from sale of businesses", "Proceeds from disposal of subsidiaries", "Proceeds from disposal of businesses", "Net cash inflow on disposal of subsidiaries", "Net cash inflow on disposal of businesses", "Disposals (net of cash disposed)", "Proceeds from disposal of controlled entities", "Divestment proceeds"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "ACF07 | Dividend paid": {
            "aliases": [],
            "calculation": "ACF47+ACF48",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF47 | Dividends paid to non-controlling interests": 
    {
        "aliases": ["Dividend paid to minorities","Dividends paid to non‑controlling interests", "Dividends paid to non‑controlling shareholders", "Dividends paid to minority interests", "Dividends paid to minority shareholders", "Cash dividends paid to non‑controlling interests", "Cash dividends paid to minority interests", "Dividend payments to non‑controlling interests", "Dividend payments to minority shareholders", "Distribution to non‑controlling interests", "Distribution to minority interests", "Cash distributions to non‑controlling interests", "Cash distributions to minority shareholders", "Dividends to non‑controlling interests", "Dividends to minority interests"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF48 | Dividends paid to owners of the parent company": 
    {
        "aliases": ["Equity Dividends", "Payments of divident to equity shareholders","Dividends paid to owners of the parent company", "Dividends paid to owners of the parent", "Dividends paid to shareholders of the parent", "Dividends paid to equity holders of the parent", "Dividends paid to ordinary shareholders", "Dividends paid to common shareholders", "Dividends paid to preference shareholders", "Cash dividends paid to shareholders", "Cash dividends paid to owners of the parent", "Cash dividends paid to equity holders", "Dividend payments to shareholders", "Dividend payments to owners of the parent", "Dividends paid (to shareholders)", "Dividends paid (to owners)", "Cash distributions to shareholders", "Cash distributions to equity holders", "Cash distributions to owners of the parent", "Equity dividends paid", "Dividends to shareholders of the company"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF49 | + Dividends from affiliates": {
        "aliases": ["Dividends from affiliates", "Dividends received from affiliates", "Dividends received from associated companies", "Dividends received from associates", "Dividends received from joint ventures", "Dividends received from subsidiaries", "Dividends from associates and joint ventures", "Dividends from equity‑accounted investees", "Dividend income from affiliates", "Dividend income from associates", "Dividend income from joint ventures", "Dividend income from investments in affiliates", "Cash dividends received from affiliates", "Cash dividends received from associates", "Cash dividends received from joint ventures", "Dividends from investments in associates/JV", "Dividends from investments in subsidiaries (when labelled as affiliates)", "Dividends received from related parties (affiliates)", "Dividend income received", "Dividends from subsidiaries / associates", "Dividends on investments received"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "ACF08 | +/- Change in Capital": {
            "aliases": [],
            "calculation": "ACF50+ACF51",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF50 | Capital inc. (dec.) - owners of the parent company": 
    {
        "aliases": ["Issuance of common/preferred shares", "Equity issuance","Capital increase/(decrease) – owners of the parent company", "Capital increase/(decrease) – owners of the parent", "Capital increases and reductions – shareholders of the parent", "Share capital increase/(decrease) – parent", "Share capital transactions – owners of the parent", "Changes in share capital – owners of the parent", "Changes in equity – owners of the parent (capital transactions)", "Issue and redemption of share capital – owners of the parent", "Issue and cancellation of shares – owners of the parent", "Equity contributions from owners of the parent", "Equity withdrawals by owners of the parent", "Capital contributions from shareholders of the parent", "Capital repayments to shareholders of the parent", "Proceeds from / repayment of equity capital – owners of the parent", "Proceeds from issue / buyback of shares – parent company", "Share buyback and issuance – parent company", "Movements in share capital – parent company (cash)", "Capital transactions with owners of the parent", "Transactions with owners of the parent in their capacity as owners (cash)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF51 | Capital inc. (dec.) - non-controlling interests": 
    {
        "aliases": ["Purchase of minority shares", "Inc/dec in minority shareholding","Capital increase/(decrease) – non‑controlling interests", "Capital increases and reductions – non‑controlling interests", "Capital increase/(decrease) – minority interests", "Share capital transactions – non‑controlling interests", "Changes in share capital – non‑controlling interests", "Changes in equity – non‑controlling interests (capital transactions)", "Equity contributions from non‑controlling interests", "Equity contributions from minority interests", "Capital contributions from minority shareholders", "Capital injections by non‑controlling shareholders", "Capital repayments to non‑controlling shareholders", "Capital repayments to minority interests", "Proceeds from capital contributions – non‑controlling interests", "Payments for capital reductions – non‑controlling interests", "Transactions with non‑controlling interests (equity, cash)", "Ownership changes with non‑controlling interests (cash component)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF09 | +/- Change in Debt": 
    {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF17 | Proceeds from the issuance of debt": 
    {
        "aliases": ["Proceeds from bank debt/bonds", "lease liabilities other debt","Proceeds from the issuance of debt", "Proceeds from issuance of debt", "Proceeds from issue of debt", "Proceeds from issuance of bonds", "Proceeds from issuance of debentures", "Proceeds from issuance of notes", "Proceeds from issuance of loan notes", "Proceeds from new borrowings", "Proceeds from long‑term borrowings", "Proceeds from short‑term borrowings", "Increase in borrowings (cash inflow)", "New loans raised", "New bank loans raised", "Cash received from new debt", "Cash proceeds from issuance of debt", "Cash proceeds from issue of debt securities", "Cash inflows from borrowings", "Proceeds from bank loans", "Proceeds from long‑term loans", "Proceeds from short‑term loans", "Proceeds from financing loans"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF18 | Repayment of detbs": 
    {
        "aliases": ["Repayments of bank debt/bonds", "lease liabilities other debt","Repayment of debts", "Repayments of debts", "Repayment of debt", "Repayment of borrowings", "Repayments of borrowings", "Repayment of loans", "Loan repayments", "Repayment of bank loans", "Repayment of bank borrowings", "Repayment of long‑term borrowings", "Repayment of short‑term borrowings", "Redemption of bonds", "Redemption of debentures", "Redemption of notes", "Redemption of loan notes", "Cash outflows for debt repayment", "Cash used to repay borrowings", "Cash used for loan repayments", "Reduction in borrowings (cash outflow)"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF52 | Loan granted to subsidiaries/JV": 
    {
        "aliases": ["Loans from parent compny/shareholders/subsidiaries","Loan granted to subsidiaries/JV", "Loans granted to subsidiaries and joint ventures", "Loans granted to subsidiaries", "Loans granted to joint ventures", "Loans to subsidiaries", "Loans to joint ventures", "Loans to group companies", "Loans to affiliated companies", "Loans to related parties (subsidiaries/JV)", "Intercompany loans granted", "Intercompany financing – loans granted", "Cash advances to subsidiaries/JV", "Cash advances to group entities", "Lending to subsidiaries/JV", "Loans and advances to subsidiaries/JV", "Loan outflows to subsidiaries/JV", "Cash outflows for loans to subsidiaries/JV"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF53 | Loan repaid from subsidiaries/JV": 
    {
        "aliases": ["Loans to parent compny/shareholders/subsidiaries","Loan repaid from subsidiaries/JV", "Loans repaid by subsidiaries and joint ventures", "Loans repaid by subsidiaries", "Loans repaid by joint ventures", "Repayment of loans from subsidiaries/JV", "Repayment of loans by subsidiaries/JV", "Cash received from loan repayments by subsidiaries/JV", "Cash received from subsidiaries/JV – loan repayment", "Intercompany loans repaid", "Intercompany financing – loans repaid", "Loan repayments from group companies", "Loan repayments from affiliated companies", "Loan repayments from related parties (subsidiaries/JV)", "Loans and advances recovered from subsidiaries/JV", "Cash inflows from loans repaid by subsidiaries/JV"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF54 | +/- Change in perimeter": 
    {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF10 | +/- Others (Currencyà)": 
    {
        "aliases": ["FX changes", "currency","Others (currency)", "Other (currency effects)", "Other (foreign exchange)", "Other currency translation effects", "Other FX translation effects", "Other foreign exchange differences on cash", "Other currency differences on cash and cash equivalents", "Other FX impact on cash", "Other exchange rate effects on cash", "Effect of currency translation on cash (other)", "Miscellaneous currency effects", "Miscellaneous foreign exchange effects", "Other non‑cash currency adjustments", "Other foreign exchange adjustments on cash flows", "Other translation differences on cash and cash equivalents", "Other exchange differences on cash", "Other items (including currency effects)", "Other reconciliation items (currency)", "Other adjustments – currency / FX"],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    # CALCULATED FIELD - Commented out (formula: see calculation field)
    "ACF11 | +/- Change in Cash": {
            "aliases": [],
            "calculation": "ACF05+ACF06+ACF07+ACF49+ACF08+ACF09+ACF52+ACF53+ACF54+ACF10",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF12 | +/- Discontinued operation": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "ACF14 | CASH AT BEGINNING OF PERIOD": {
            "aliases": [],
            "calculation": "ACF11+ACF12",
            "is_calculated": True,
        "indent_level": 0
        },

    "ACF13 | CASH AT END OF PERIOD": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "CAFF1 | CAF": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

}


# ---------------------------------------------------------------------------
# APAC Income Statement Fields (Q-prefix codes)
# Same aliases as US fields, different codes per Income_Statement_Fields.xlsx
# ---------------------------------------------------------------------------

APAC_INCOME_STATEMENT_FIELDS = {
        "Q1 | Sales (Revenue)": {
        "aliases": [
            "Revenue",
            "Revenue from operations",
            "Operating Revenue",
            "Total revenue",
            "Total Sales",
            "Turnover",
            "Net Sales",
            "Gross sales",
            "Operating Income",
            "Sales revenue",
            "Sales"
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q51 | o/w  Net Rental income": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q3 | Other Revenues,  including other operating income from patents & licensing": 
     {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    
    "Q93 | Total Revenue - TRR": 
     {
        "aliases": [],
        "calculation": "Q1+Q3",
        "is_calculated": True,
        "indent_level": 0
    },
    
    
        "Q5 | Cost of sales": {
        "aliases": [
            "Cost of Sales",
            "Cost of Revenue",
            "Cost of Services",
            "Cost of Products Sold",
            "Direct Costs of Sales",
            "Manufacturing Costs",
            "Fuel",
            "Power",
            "Service Maintenance Expense",
            "Cost of sales"
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q47 | GROSS PROFIT - TRR31": 
     {
        "aliases": [],
        "calculation": "Q93-Q5",
        "is_calculated": True,
        "indent_level": 0
    },
    

    "Q6 | SG&A Expense": {
        "aliases": [],  # No direct aliases - this is a calculated field
        "calculation": "Q48+Q49+Q50",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q48 | o/w administratives expenses": {
        "aliases": [
            "General and Administrative Expenses (G&A Expenses)",
            "Administrative Costs",
            "Administration and other expenses"
            "Operating Expenses",
            "Selling",
            "General & Administrative Expenses (SG&A Expenses)",
            "Corporate Expenses",
            "Overhead Costs",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q49 | o/w distribution costs, advertising & promotion": {
        "aliases": [
            "Selling Expenses",
            "Logistics Costs",
            "Fulfillment Costs",
            "Delivery Expenses",
            "Shipping Costs",
            "Marketing and Distribution Costs",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q50 | o/w Salaries & related costs": {
        "aliases": [
            "Personnel Costs",
            "Employee Compensation",
            "Wages and Salaries",
            "Staff Costs",
            "Employee Costs",
            "Payroll Expenses",
            "Salaries",
            "Wages and Benefits",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },


    "Q7 | External operating costs (incl. services, R&D)": {
        "aliases": [
            "Research and Development Expenses",
            "R&D Expenses",
            "Development Costs",
            "Innovation Costs",
            "New Product Development Costs",
            "Technology Development Expenses",
            "Exploration and Development Cost",
            "Service Expense",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q8 | Taxes other than income tax": {
        "aliases": [
            "Property Taxes",
            "Local Taxes",
            "Business Taxes",
            "Excise Taxes",
            "Levies",
            "Other Statutory Taxes",
            "Insurance Premiums",
            "Insurance Costs",
            "Policy Costs",
            "Insurances",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q14 | Net depreciation and amortization expense": {
        "aliases": [
            "D&A",
            "Depreciation",
            "Amortisation",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    
    "Q105 | D&A of right of use Assets IFRS16": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    
    "Q14init | o/w net depreciation and amortization expense pre-IFRS16": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q97 | Net charges to provisions and impairment losses": {
        "aliases": [
            "Provision for Doubtful Accounts",
            "Allowance for Doubtful Accounts Expense",
            "Doubtful Accounts Expense",
            "Uncollectible Accounts Expense",
            "Impairment Losses on Receivables",
            "Write-offs of Receivables",
            "Credit Losses Expense",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q9 | +/-Changes in inventories and property development inventories": {
        "aliases": [
            "Increase/Decrease in Inventories",
            "Inventory Movements",
            "Changes in Stock",
            "Adjustments for Inventory",
            "Changes in Property Development Stock",
            "Property Development Inventory Movements",
            "Work-in-Progress on Property Development",
            "Development Properties - Changes",
            "Alias",
            "Changes in Inventories and Development Properties",
            "Inventory and Property Stock Adjustments",
            "Movements in Operating Assets - Inventories and Properties",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "Q10 | +/- Other revenues & expenses from current operations": {
        "aliases": [
            "Other Operating Expenses",
            "Miscellaneous Expenses",
            "General Expenses",
            "Operating Costs - Other",
            "Ancillary Expenses",
            "Non-primary Operating Expenses",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q20 | EBIT": {
        "aliases": [],
        "calculation": "Q47-Q6-Q7-Q8-Q14-Q97+Q9+Q10",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q104 | Current EBITDA": {
        "aliases": [],
        "calculation": "Q20+Q14+Q97",
        "is_calculated": True,
        "indent_level": 0
    },

    "ITRR225 | EBITDA IFRS16 [Current EBITDA + D&A of right of use Assets]": {
        "aliases": [],
        "calculation": "Q20+Q14+Q97",
        "is_calculated": True,
        "indent_level": 0
    },


    "Q22 | Goodwill Impairment (non recurring)": {
        "aliases": [
            "Goodwill Impairment Charge",
            "Impairment Loss - Goodwill",
            "Write-down of Goodwill",
            "Goodwill Impairment Expense",
            "Impairment of Intangible Assets - Goodwill",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q23 | Grants and assistance": {
        "aliases": [
            "Government Grants",
            "Subsidies",
            "Grants Received",
            "Assistance Received",
            "Public Funding",
            "Non-repayable Grants",
            "Economic Development Grants",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "Q56 | Restructuring costs & similar items": {
        "aliases": [
            "Restructuring Charges",
            "Restructuring Expenses",
            "Restructuring Gains/(Losses)",
            "Reorganization Costs/Gains",
            "Severance Costs",
            "Closure Costs",
            "Business Transformation Costs",
            "Asset Impairment & Restructuring Charges",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q57 | Disposals of intangibles, subsidiaries and affiliates (gains or losses)": {
        "aliases": [
            "Sale of Intangible Assets",
            "Gain (loss) on Sale of Intangible Assets",
            "Write-off of Intangible Assets",
            "Pour \"Disposals of subsidiaries",
            "Sale of Subsidiaries",
            "Gain (loss) on Disposal of Subsidiaries",
            "Divestitures of Subsidiaries",
            "Sale of Business Units",
            "Sale of Investments in Associates",
            "Gain (loss) on Sale of Investments in Affiliates",
            "Divestment of Associates",
            "Gain (loss) on Sale of Assets",
            "Discontinued Operations",
            "Divestitures and Other Asset Disposals",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "Q58 | Disposals of property, plant and equipment (gains or losses)": {
        "aliases": [
            "Sale of Property",
            "Plant and Equipment (PP&E)",
            "Gain (loss) on Sale of PP&E",
            "Proceeds from Sale of Fixed Assets",
            "Disposal of Fixed Assets",
            "Write-off of Property",
            "Plant and Equipment",
            "Sale of Assets",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q60 | Share options granted to directors and employees": {
        "aliases": [
            "Employee Stock Options",
            "Director Stock Options",
            "Share-Based Compensation Expense",
            "Equity-Settled Share-Based Payments",
            "Stock Option Awards",
            "Long-Term Incentive Plans (LTIP)",
            "Executive Stock Options",
            "Awards of Stock Options to Management",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    
    "Q74 | +/- Interests in jointly controlled entities (Equity affiliates, JVs)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q24 | +/- Other non recurring operating incomes and costs": {
        "aliases": [
            "Other Income/(Expenses)",
            "Miscellaneous Income/(Expenses)",
            "Finance Income/(Costs)",
            "Investment Income/(Losses)",
            "Gain (loss) from Unusual Items",
            "Exceptional Items Gain (Loss)",
            "Discontinued Operations",
            "Significant One-time Events Gain (Loss)",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },


    "Q52 | Exceptional items": {
        "aliases": [],
        "calculation": "Q22+Q23+Q56+Q57+Q58+Q60+Q74+Q24",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q26 | EBIT (including exceptional items)": {
        "aliases": [],
        "calculation": "Q20+Q52",
        "is_calculated": True,
        "indent_level": 0
    },


    "Q27 | Incomes from bank deposits and alikes": {
        "aliases": [
            "Financial Income",
            "Int on Bank Deposits",
            "Interest income",
            "Interest Revenue",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q28 | Interest costs (gross)": {
        "aliases": [
            "Finance Costs",
            "Borrowing Costs",
            "Interest Expense on Debt",
            "Cost of Borrowing",
            "Finance Charges",
            "Interest Paid",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q29 | o/w from hybrid instruments, convertible bonds": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q29ifrs16 | o/w lease interest IFRS16": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q28init | o/w interest costs (gross) pre-IFRS16": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 1
    },

    "Q30 | Cost of net financial debt": {
        "aliases": [],
        "calculation": "Q28-Q27",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q100 | +/- FX gains (loss) - realized": {
        "aliases": [
            "Foreign Exchange Gain (Loss)",
            "Foreign Currency Gain (Loss)",
            "Net Foreign Exchange Gain (Loss)",
            "Foreign Exchange Differences",
            "Unrealized/Realized Foreign Exchange Gain (Loss)",
            "Exchange Rate Differences",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q101 | +/- FX gains (loss) - unrealized": {
        "aliases": [],
        "calculation": "",
        "is_calculated": False,
        "indent_level": 0
    },

    "Q31 | +/- Gain (loss) on extinguishment of debt": {
        "aliases": [
            "Gain (loss) on debt redemption",
            "Gain (loss) on early retirement of debt",
            "Gain (loss) from debt restructuring",
            "Gain (loss) on settlement of debt",
            "Debt extinguishment income (expense)",
            "Gain (loss) from financial liabilities extinguishment",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    
    "Q33 | Net annual prov. for current and long term fin. assets": {
        "aliases": [],
        "calculation": "",
        "is_calculated": False,
        "indent_level": 0
    },

        
    "Q94 | +/- Other investment & financial income and expenses (incl prov. for financial assets)": {
        "aliases": [],
        "calculation": "",
        "is_calculated": False,
        "indent_level": 0
    },

        
    "Q34 | Financial income": {
        "aliases": [],
        "calculation": "Q30+Q100+Q101+Q31+Q33+Q94",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q70 | Profit before income tax (PBT)": {
        "aliases": [],
        "calculation": "Q26+Q34",
        "is_calculated": True,
        "indent_level": 0
    },


    "Q36 | +/- Interests in jointly controlled entities": {
        "aliases": [
            "Equity in losses of equity method investees",
            "Income/Loss from Equity Affiliates/Associates",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q102 | +/- Other non-operating income (loss)": {
        "aliases": [],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },
    
    "Q35 | Income tax": {
        "aliases": [
            "Income Tax Expense/(Benefit)",
            "Income tax expense",
            "Income Tax Expense",
            "Current Tax Expense/(Benefit)",
            "Deferred Tax Expense/(Benefit)",
            "Provision for Taxes on Income",
            "Corporate Income Tax Expense/(Benefit)",
            "Taxes on Income",
            "Taxation",
            "Income Tax Charge",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q37 | Profit for the year from continuing operations": {
        "aliases": [],
        "calculation": "Q70+Q36+Q102+Q35",
        "is_calculated": True,
        "indent_level": 0
    },
    
    "Q38 | +/- Net profit/(loss) from discontinued and held-for-sale operations": {
        "aliases": [
            "Net gain (loss) on disposal of business operations",
            "Profit (loss) from operations held for sale",
            "Results of discontinued segments",
            "Gain (loss) from divested activities",
            "Net impact of discontinued operations",
            "Income (loss) from segments to be disposed of",
        ],
        "calculation": None,
        "is_calculated": False,
        "indent_level": 0
    },

    "Q39 | Net profit (loss) for the year": {
        "aliases": [
            "Net profit",
            "Profit",
            "Profit After tax",
        ],
        "calculation": "Q37+Q38",
        "is_calculated": True,
        "indent_level": 0
    },

    "Q41 | Profit attributable to owners of the Group": {
        "aliases": [],
        "calculation": "",
        "is_calculated": False,
        "indent_level": 0
    },

    
    "Q42 | Profit attributable to Non-controlling interests": {
        "aliases": [],
        "calculation": "",
        "is_calculated": False,
        "indent_level": 0
    }

}




# ---------------------------------------------------------------------------
# APAC Balance Sheet Fields (U-prefix codes)
# Combined from Assets and Liabilities sheets as per PDF extraction logic
# Same aliases as US fields, different codes for APAC region
# ---------------------------------------------------------------------------

APAC_BALANCE_SHEET_FIELDS = {
  
  #### Asset Section ########

  "U16 | Goodwill (\"net\")": {
    "aliases": ["No other alias"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U10 | Other Intangible assets Net": {
    "aliases": ["Other intangible assets, net", "Intangible assets (excluding goodwill)", "Intangible assets, net", "Net intangible assets", "Intangible assets – other", "Other intangibles, net book value", "Intangible fixed assets (excluding goodwill)", "Intangible assets – software, licenses, patents, trademarks", "Software and other intangible assets", "Licences, patents, trademarks and similar rights", "Capitalised development costs (intangible assets)", "Intangible operating assets (excluding goodwill)", "Intangible non‑current assets (other)", "Other intangible non‑current assets"],
    "calculation": "U11+U12+U13+U14",
    "is_calculated": True,
        "indent_level": 0
  },

  "U11 | o/w non impaired intangible assets": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U12 | o/w impaired intangible assets": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U13 | o/w new intangible assets": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U14 | o/w provisions for fixed assets (non financial)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U2 | Tangible assets: Property, plant and equipment": {
    "aliases": ["Property, plant and equipment", "PPE", "Tangible assets", "Tangible fixed assets", "Fixed assets – tangible", "Land, buildings, plant and equipment", "Plant and equipment", "Buildings and machinery", "Property and equipment", "Property, plant and machinery", "Net property, plant and equipment", "Property, plant and equipment, net", "Net tangible assets (if used specifically for PPE)", "Non‑current tangible assets", "Operating fixed assets", "Industrial fixed assets", "Right‑of‑use assets", "Right of use assets", "Right‑of‑use asset", "Right of use asset", "Lease right‑of‑use assets", "Lease assets – right‑of‑use", "Leased right‑of‑use assets", "Assets under lease (right‑of‑use)", "ROU assets", "ROU asset", "Use‑of‑asset rights (lease)", "Right‑of‑use property, plant and equipment", "Right‑of‑use PPE", "Right‑of‑use buildings", "Right‑of‑use land and buildings", "Right‑of‑use vehicles", "Right‑of‑use equipment", "Right‑of‑use assets – non‑current", "Lease assets recognized under IFRS 16", "Lease assets recognized under ASC 842"],
    "calculation": "U4+U3+U5+U115+U116+U201",
    "is_calculated": True,
        "indent_level": 0
  },

    "U4 | o/w Amortization/Depreciation of fixed assets (\"impaired assets\")": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

 "U3 | o/w Property, plant and equipment Net (\"non impaired assets\")": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U5 | o/w new Property, Plant and equipment": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U115 | o/w Construction in progress": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
    "U116 | o/w Long term advances given (prepayments)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "U201 | Rigth of use IFRS16": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U6 | Investment properties": {
    "aliases": ["Investment properties", "Investment property", "Properties held for investment", "Real estate investments", "Property investments", "Properties held to earn rentals", "Properties held for capital appreciation", "Real estate held for investment", "Investment real estate", "Non‑current investment properties", "Investment properties, net", "Investment property, net book value"],
    "calculation": "U7+U8+U9",
    "is_calculated": True,
        "indent_level": 0
  },

  "U7 | o/w impaired investment properties": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U8 | o/w impaired investment properties": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U9 | o/w new investment properties": {
    "aliases": ["Exploration and evaluation assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
    
  "U18 | Employee benefits": {
    "aliases": ["Employee benefits (assets)", "Employee benefit assets", "Net defined benefit asset", "Surplus of defined benefit plan", "Pension plan assets (net)", "Retirement benefit assets", "Asset for employee benefits", "Employee benefit plan surplus", "Reimbursement rights under employee benefit plans", "Non‑current employee benefit assets", "Long‑term employee benefit assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U17 | Non current financial assets (incl derivatives & hedging financial instruments)": {
    "aliases": ["Other Financial Assets", "Derivative Assets","Non‑current financial assets", "Long‑term financial assets", "Financial assets – non‑current", "Financial assets – long‑term", "Non‑current investments", "Long‑term investments", "Non‑current loans and receivables (financial)", "Long‑term loans and receivables", "Non‑current marketable securities", "Long‑term marketable securities", "Non‑current financial investments", "Derivative financial assets – non‑current", "Derivative assets – non‑current", "Hedging instruments – assets (non‑current)", "Financial instruments – non‑current assets", "Financial assets at fair value (non‑current)", "Available‑for‑sale financial assets – non‑current (legacy term)", "Held‑to‑maturity investments – non‑current", "Other non‑current financial assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U19 | o/w provisions for fixed financial assets": {
    "aliases": ["Financial investments"],
    "calculation": None,
    "is_calculated": False,
    "indent_level": 1
  },

  "U20 | Equity affiliates: non-consolidated investments and loans (Net)": {
    "aliases": ["Interest in net assets of joint ventures","Investments in subsidiaries or non-controlled affiliates","Equity affiliates (net)", "Investments in equity affiliates (net)", "Investments in associates (net)", "Investments in joint ventures (net)", "Investments in associates and joint ventures (net)", "Investments in equity‑accounted investees (net)", "Equity‑accounted investments (net)", "Non‑consolidated investments (equity method)", "Non‑consolidated affiliates – investments and loans (net)", "Investments in related companies (equity affiliates)", "Investments in associated companies (net)", "Investments in jointly controlled entities (net)", "Loans to equity affiliates (net)", "Loans to associates and joint ventures (net)", "Investments and loans to affiliates (net)", "Affiliated companies – investments and loans (net)"],
    "calculation": None,
    "is_calculated": False,
    "indent_level": 0
  },

  "U88 | Other non-current assets": {
    "aliases": ["Investments", "Other non-current assets","Other non‑current assets", "Non‑current assets – other", "Other long‑term assets", "Long‑term assets – other", "Miscellaneous non‑current assets", "Sundry non‑current assets", "Other non‑current operating assets", "Other non‑current receivables", "Other long‑term receivables", "Other long‑term prepayments", "Other long‑term deposits and guarantees (assets)", "Other non‑current deferred charges", "Other non‑current assets (net)", "Non‑current sundry assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U21 | Deferred tax assets and tax receivables /recoverable": {
    "aliases": ["Claim for tax returns and prepaid taxes","Deferred tax assets and tax receivables", "Deferred tax assets and income tax receivables", "Deferred tax assets and recoverable taxes", "Deferred income tax assets and tax receivables", "Deferred tax assets and taxes recoverable", "Deferred tax assets", "Deferred income tax assets", "Tax receivables", "Income tax receivables", "Recoverable income taxes", "Recoverable tax assets", "Tax assets – deferred and current receivables", "Deferred tax and tax receivables", "Deferred tax assets and other tax assets", "Deferred tax assets and tax credits", "Deferred tax assets and recoverable VAT/GST (if grouped)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U1 | NON CURRENT ASSETS": {
    "aliases": [],
    "calculation": "U16+U10+U2+U6+U18+U17+U20+U88+U21",
    "is_calculated": True,
    "indent_level": 0
  },

    "U24 | Inventories (net)": {
    "aliases": ["Stock", "Raw Materials"],
    "calculation": "U25+U26+U27+U28",
    "is_calculated": True,
        "indent_level": 0
  },

  "U25 | o/w Raw materials and supplies": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U26 | o/w In-process goods": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U27 | o/w Finished and semi-finished products": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U28 | o/w obsolescence allowance": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
    "indent_level": 1
  },

    "U114 | Advances and down-payments (on current assets)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
    "indent_level": 0
  },


    "U200 | Trade receivables": {
    "aliases": ["Accounts Receivables"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U31 | Other receivables": {
    "aliases": ["Other Receivables"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U32 | o/w discounted bills": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U34 | Provisions for debtors": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U29 | Trade receivables & other debtors Net (minus provision for impairment)": {
    "aliases": [],
    "calculation": "U20+U31-U34",
    "is_calculated": True,
        "indent_level": 0
  },

  "U98 | Other current operating assets": {
    "aliases": ["Other current operating assets", "Other current assets (operating)", "Other current non‑financial assets", "Current operating assets – other", "Miscellaneous current operating assets", "Sundry current operating assets", "Other current receivables (operating)", "Other current non‑trade receivables", "Prepayments and other current assets", "Prepaid expenses (current) and other current assets", "Prepaid operating expenses (current)", "Other current operating receivables", "Other current operating prepayments", "Other current assets – non‑financial", "Other current assets (excluding cash and financial)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U101 | o/w Derivatives relating to operating activities": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "U36 | Current financial assets": {
    "aliases": ["Current financial assets", "Short‑term financial assets", "Financial assets – current", "Financial assets – short‑term", "Current investments", "Short‑term investments", "Marketable securities – current", "Trading securities – current", "Financial instruments – current assets", "Derivative financial assets – current", "Derivative assets – current", "Hedging instruments – assets (current)", "Current loans and receivables (financial)", "Short‑term loans and receivables (financial)", "Current financial assets at fair value", "Current financial assets held for trading", "Current financial investments"],
    "calculation": "U103+U104+U37+U106+U107+U108",
    "is_calculated": True,
        "indent_level": 0
  },
     
    "U103 | o/w Loans and other receivables": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U104 | o/w Other financial assets (incl derivatives)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U37 | o/w Other current assets (incl.short term investment)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U106 | o/w Tax and employee-related receivables": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U107 | o/w Receivable from Group and shareholders": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U108 | o/w Prepaid expenses": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "U35 | Current tax asset to recover (receivable)": {
    "aliases": ["Current tax asset to recover", "Current income tax receivable", "Income tax receivable – current", "Current tax receivables", "Tax receivables – current", "Taxes recoverable – current", "Recoverable income taxes – current", "Corporate income tax receivable (current)", "Current tax assets", "Current tax assets – income tax", "Current tax refunds receivable", "Income tax recoverable (current)", "Tax credit receivable (current, income tax)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

   "U38 | Provisions for current financial assets": {
    "aliases": ["Provisions for current financial assets", "Provision for impairment of current financial assets", "Provision for expected credit losses – current financial assets", "Allowance for impairment of current financial assets", "Allowance for losses on current financial assets", "Loss allowance – current financial assets", "Provision for doubtful financial assets (current)", "Provision for impairment – current investments", "Provision for impairment – current loans and receivables", "Provision for impairment – current marketable securities", "Provision for impairment of short‑term financial assets", "Impairment reserve for current financial assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U39 | Net Cash and bank deposits (excluding overdrafts)": {
    "aliases": ["Cash & Cash Equivalents", "Cash & Short Term Investments","Net cash and bank deposits (excluding overdrafts)", "Cash and bank deposits (excluding overdrafts)", "Cash and cash equivalents (excluding bank overdrafts)", "Cash and cash equivalents", "Cash and bank balances", "Cash at bank and in hand", "Cash and short‑term deposits", "Cash and term deposits (short‑term)", "Cash and bank accounts", "Cash on hand and at bank", "Bank deposits (current, unrestricted)", "Cash balance (excluding overdrafts)", "Cash and restricted cash (if grouped)", "Cash equivalents (money market funds, etc.)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U149 | o/w other cash equivalents considered as restricted cash": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  
  "U40 | Provisions for cash and bank deposits": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "U22 | CURRENT ASSETS": {
    "aliases": [],
    "calculation": "U24+U114+U29+U98+U36+U35+U38+U39",
    "is_calculated": True,
        "indent_level": 0
  },

  "U23 | Assets held-for-sale / available-for-sale": {
    "aliases": ["Assets held for sale", "Assets classified as held for sale", "Non‑current assets held for sale", "Disposal group assets held for sale", "Assets of disposal groups classified as held for sale", "Assets of disposal group", "Held‑for‑sale assets", "Assets held‑for‑sale / available‑for‑sale", "Assets available for sale (legacy phrasing)", "Available‑for‑sale assets (non‑current)", "Assets held for sale and discontinued operations", "Assets associated with discontinued operations (held for sale)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U41init | TOTAL ASSET pre-IFRS16": {
    "aliases": [],
    "calculation": "U1+U22-U201",
    "is_calculated": True,
        "indent_level": 0
  },

  "U41 | TOTAL ASSET IFRS16": {
    "aliases": [],
    "calculation": "U1+U22",
    "is_calculated": True,
        "indent_level": 0
  },

##### Liabiilities Section ###########

  "U44 | Share capital": {
    "aliases": ["Common equity", "Issued and paid-Up Shares"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U45 | Share premium": {
    "aliases": ["Additional Equity"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U46 | o/w premium on hybrid borrowings": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U181 | Reserves": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U173 | o/w Translation reserves": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

      "U174 | Treasury shares": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

        "U172 | Debts classified as- or converted into equity (MCS, perpetual securities…)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U47 | Revaluation surplus": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

   "U48 | Retained earnings (revenue reserve)": {
    "aliases": ["No other Alias"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U49 | Net income (accumulated other comprehensive income)": {
    "aliases": ["Other comprehensive income loss"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U43 | Equity attributable to owners of the company": {
    "aliases": [],
    "calculation": "U44+U45+U181+U174+U172+U47+U48+U49",
    "is_calculated": True,
        "indent_level": 0
  },
  "U51 | Non-controlling / Minority interests": {
    "aliases": ["No other Alias"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U161 | TOTAL EQUITY": {
    "aliases": [],
    "calculation": "U43+U51",
    "is_calculated": True,
        "indent_level": 0
  },

  "U53 | Borrowings / debt >1 yr (excluding current maturities of LT debt)": {
    "aliases": ["Bank Debt", "Bonds", "Lease Liabilities","Long‑term borrowings", "Long‑term debt", "Non‑current borrowings", "Non‑current financial liabilities – borrowings", "Non‑current interest‑bearing liabilities", "Long‑term loans", "Long‑term bank loans", "Long‑term bank borrowings", "Long‑term loans payable", "Long‑term notes payable", "Long‑term bonds payable", "Bonds payable (non‑current)", "Debentures (non‑current)", "Loan notes (non‑current)", "Term loans (non‑current portion)", "Non‑current portion of borrowings (excluding current maturities)", "Non‑current portion of loans payable", "Non‑current portion of bank loans", "Non‑current portion of bonds / debentures", "Interest‑bearing borrowings – non‑current", "Debt due after one year", "Borrowings due after 12 months", "Loans due after one year", "Long‑term interest‑bearing debt", "Long‑term financial obligations", "Long‑term financing liabilities", "Finance lease liabilities – non‑current", "Capital lease obligations – non‑current", "Capital lease liabilities – non‑current", "Lease liabilities – non‑current (finance/capital)", "Long‑term finance lease liabilities", "Long‑term capital lease obligations", "Non‑current portion of finance lease liabilities", "Non‑current portion of capital lease obligations", "Non‑current portion of lease liabilities (finance/capital)", "Lease obligations due after one year – finance leases", "Long‑term lease debt (finance leases)", "Finance lease debt – non‑current", "Capital lease debt – non‑current", "Plain vanilla bonds (non‑current)", "Bonds payable – non‑current", "Bonds – non‑current", "Straight bonds (non‑current)", "Non‑convertible bonds (non‑current)", "Non‑derivative bonds (non‑current)", "Standard bonds (non‑current)", "Plain vanilla debentures (non‑current)", "Plain vanilla notes (non‑current)", "Long‑term bonds payable", "Long‑term debentures", "Long‑term notes payable (bond‑type)", "Bonds due after one year", "Debentures due after one year", "Notes due after one year (fixed‑rate, plain vanilla)", "Medium‑term notes (non‑current portion)", "Senior unsecured bonds (non‑current)", "Hybrid borrowings (non‑current)", "Hybrid debt (non‑current)", "Hybrid financial instruments (non‑current liabilities)", "Hybrid capital instruments (non‑current liabilities)", "Convertible bonds (non‑current)", "Convertible debt (non‑current)", "Convertible notes (non‑current)", "Convertible loan notes (non‑current)", "Convertible debentures (non‑current)", "Convertible securities (liability component, non‑current)", "Long‑term convertible bonds", "Long‑term convertible debt", "Long‑term hybrid borrowings", "Long‑term hybrid capital instruments (classified as debt)", "Perpetual notes (liability‑classified hybrids)", "Perpetual bonds (hybrid liabilities)", "Mezzanine debt instruments (if labelled hybrid borrowings)", "Subordinated debt (non‑current)", "Subordinated borrowings (non‑current)", "Subordinated loans (non‑current)", "Subordinated liabilities (non‑current)", "Long‑term subordinated debt", "Long‑term subordinated borrowings", "Subordinated notes (non‑current)", "Subordinated bonds (non‑current)", "Subordinated loan notes (non‑current)", "Junior debt (non‑current)", "Junior subordinated debt (non‑current)", "Perpetual subordinated debt (if classified as liability)", "Debt subordinated to other creditors (non‑current)", "Subordinated financial liabilities – non‑current", "Subordinated interest‑bearing liabilities – non‑current"],
    "calculation": "U53+U182",
    "is_calculated": True,
        "indent_level": 0
  },

    "U53init | o/w Borrowings / debt >1 yr (excluding current maturities of LT debt) pre-IFRS16": {
    "aliases": ["Bank Debt", "Bonds", "Lease Liabilities"],
    "calculation": "U54+U55+U56+U57",
    "is_calculated": True,
        "indent_level": 0
  },

  "U54 | o/w  subordinated debt": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U55 | o/w  hybrid borrowings / convertible bonds": {
    "aliases": ["o/w  Term loan","o/w  Revolver","o/w  other LT debt"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },


  "U56 | o/w  plain vanilla bonds": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U57 | o/w  (finance or capital) lease": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "U182 | Lease liabilities current  IFRS16": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
    "U58 | Deferred income tax liabilities and non-current tax liabilities": {
    "aliases": ["Deferred income tax liabilities", "Deferred tax liabilities", "Deferred taxation liabilities", "Deferred income taxes (liabilities)", "Deferred income tax – non‑current", "Non‑current tax liabilities", "Non‑current income tax liabilities", "Long‑term tax liabilities", "Long‑term income tax liabilities", "Deferred tax – liabilities (non‑current)", "Deferred tax liabilities and non‑current tax liabilities", "Deferred income taxes and long‑term tax liabilities", "Deferred tax obligations", "Future income tax liabilities", "Provision for deferred tax (liabilities)", "Deferred taxation – non‑current liabilities"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U59 | Provisions for pensions and employee benefits": {
    "aliases": ["Provisions for pensions and employee benefits", "Pension provisions", "Provision for pensions", "Pension liabilities", "Retirement benefit obligations", "Post‑employment benefit obligations", "Defined benefit obligation (DBO)", "Defined benefit pension liabilities", "Employee benefit obligations – non‑current", "Non‑current employee benefit obligations", "Long‑term employee benefit liabilities", "Long‑term employee benefit provisions", "Provisions for employee benefits – non‑current", "Provision for post‑employment benefits", "Provision for retirement benefits", "Provision for gratuity (non‑current portion)", "Provision for long‑service awards (non‑current)", "Provision for other long‑term employee benefits", "Pension and other employee benefit obligations", "Pension and other long‑term employee benefit provisions"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U175 | Other non-current provisions": {
    "aliases": ["Other non‑current provisions", "Other long‑term provisions", "Non‑current provisions (other)", "Long‑term provisions (other)", "Provisions for risks and charges – non‑current", "Provisions for risks and charges – long‑term", "Provisions for restructuring (non‑current portion)", "Restructuring provision – non‑current", "Provisions for legal claims (non‑current portion)", "Legal provisions – non‑current", "Provisions for environmental liabilities (non‑current)", "Environmental provision – non‑current", "Provisions for onerous contracts (non‑current)", "Onerous contract provisions – non‑current", "Provisions for guarantees (non‑current)", "Guarantee provisions – non‑current", "Other long‑term risk provisions", "Other provisions for contingencies – non‑current", "Other provisions – long‑term"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U60 | Other non-current liabilities": {
    "aliases": ["Other non-current liabilities", "Derivative Liabilities", "Other Financial Liabilities", "Loans from shareholders/related parties","Other non‑current liabilities", "Other long‑term liabilities", "Non‑current liabilities – other", "Long‑term liabilities – other", "Miscellaneous non‑current liabilities", "Miscellaneous long‑term liabilities", "Other non‑current financial liabilities (non‑borrowings)", "Other long‑term financial liabilities", "Non‑current deferred income", "Deferred income – non‑current", "Long‑term deferred revenue", "Long‑term contract liabilities", "Non‑current lease liabilities (if not separately presented)", "Long‑term lease obligations (if grouped)", "Non‑current liabilities to related parties (non‑trade)", "Long‑term payables to related parties", "Long‑term liabilities – government grants (deferred)", "Long‑term liabilities – contingent consideration", "Other non‑current obligations", "Other long‑term obligations"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U179 | o/w Loan from shareholders or related parties": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U52init | NON CURRENT LIABILITIES pre-IFRS16": {
    "aliases": [],
    "calculation": "U53+U58+U59+U175+U60-U182",
    "is_calculated": True,
        "indent_level": 0
  },

    "U52 | NON CURRENT LIABILITIES": {
    "aliases": [],
    "calculation": "U53+U58+U59+U175+U60",
    "is_calculated": True,
        "indent_level": 0
  },

   "U63 | Borrowings / debt < 1 yr (including current maturities of LT debt)": {
    "aliases": ["Financial Debt", "Bank debt", "Bonds", "Lease Liabilities","Short‑term borrowings", "Short‑term debt", "Commercial paper", "Current borrowings", "Current financial liabilities – borrowings", "Bank overdrafts and short‑term borrowings", "Bank overdrafts (if not treated as cash)", "Short‑term bank loans", "Short‑term bank borrowings", "Current portion of long‑term debt", "Current portion of long‑term borrowings", "Current maturities of long‑term debt", "Current maturities of long‑term borrowings", "Current portion of loans payable", "Current portion of bank loans", "Current portion of bonds payable", "Current portion of notes payable", "Current installment of long‑term loans", "Current portion of debentures", "Current portion of term loans", "Short‑term portion of interest‑bearing debt", "Debt due within one year", "Borrowings due within 12 months", "Loans due within one year", "Current financial borrowings", "Current interest‑bearing liabilities", "Current financial obligations – borrowings", "Short‑term interest‑bearing loans and borrowings", "Bank overdraft", "Bank overdrafts", "Overdrafts with banks", "Bank overdraft facility used", "Overdrawn bank accounts", "Cash at bank – overdrawn", "Bank current account overdraft", "Overdrawn current accounts", "Short‑term bank overdraft", "Bank overdraft – current liability", "Overdrafts and short‑term borrowings (if combined)", "Bank overdraft balances (liabilities)", "Bank overdraft and similar facilities", "Current portion of long‑term debt", "Current portion of long‑term borrowings", "Current maturities of long‑term debt", "Current maturities of long‑term borrowings", "Current portion of loans payable (long‑term loans)", "Current portion of bank loans (long‑term)", "Current portion of bonds payable (long‑term bonds)", "Current portion of debentures (long‑term debentures)", "Current portion of notes payable (long‑term notes)", "Current portion of term loans", "Long‑term debt due within one year", "Long‑term borrowings due within one year", "Installments of long‑term debt due in the next year", "Next‑year maturities of long‑term loans", "Short‑term portion of long‑term borrowings", "Short‑term portion of long‑term debt", "Finance lease liabilities – current", "Capital lease obligations – current", "Capital lease liabilities – current", "Lease liabilities – current (finance leases)", "Current portion of finance lease liabilities", "Current portion of capital lease obligations", "Current portion of lease liabilities (finance/capital)", "Short‑term finance lease liabilities", "Short‑term capital lease obligations", "Current lease obligations (finance/capital)", "Lease payments due within one year – principal (finance leases)", "Current portion of lease debt (finance leases)", "Finance lease debt – current", "Capital lease debt – current", "Discounted bills (current)", "Discounted trade bills", "Discounted bills of exchange", "Discounted promissory notes", "Bills discounted with banks", "Bills of exchange discounted", "Discounted receivables (with recourse)", "Factored receivables with recourse (if labelled as discounted bills)", "Discounted trade notes", "Discounted customer bills", "Discounted bills payable (if treated as borrowing)", "Short‑term borrowings – discounted bills", "Bills discounting facility", "Trade bill discounting", "Plain vanilla bonds (current)", "Bonds payable – current portion", "Bonds – current portion", "Straight bonds (current portion)", "Non‑convertible bonds – current portion", "Non‑derivative bonds – current portion", "Standard bonds (current portion)", "Plain vanilla notes (current)", "Plain vanilla debentures (current)", "Current portion of bonds", "Current portion of debentures", "Current portion of notes payable (bond‑type)", "Short‑term bonds payable", "Short‑term debentures", "Short‑term notes (bond‑type)", "Bonds due within one year", "Debentures due within one year", "Notes due within one year (fixed‑rate / plain vanilla)", "Hybrid borrowings (current)", "Hybrid debt (current)", "Hybrid instruments (current liabilities)", "Convertible bonds (current portion)", "Convertible debt (current portion)", "Convertible notes (current portion)", "Convertible loan notes (current portion)", "Convertible securities (current liability portion)", "Convertible debentures (current portion)", "Hybrid capital instruments (current liabilities)", "Perpetual and other hybrid instruments (current portion, if classified as debt)", "Current portion of convertible bonds", "Current portion of convertible debt", "Current portion of hybrid borrowings", "Short‑term convertible debt", "Short‑term convertible bonds", "Short‑term hybrid debt instruments", "Subordinated debt (current)", "Subordinated borrowings (current)", "Subordinated loans (current)", "Subordinated liabilities (current)", "Short‑term subordinated debt", "Short‑term subordinated borrowings", "Subordinated notes (current)", "Subordinated bonds (current portion)", "Subordinated loan notes (current)", "Junior debt (current)", "Junior subordinated debt (current)", "Debt subordinated to other creditors (current)", "Subordinated financial liabilities – current", "Subordinated interest‑bearing liabilities – current"],
    "calculation": "U63+U183",
    "is_calculated": True,
        "indent_level": 0
  },

    "U63init | o/w Borrowings / debt < 1 yr (including current maturities of LT debt) pre-IFRS16": {
    "aliases": [],
    "calculation": "U64+U65+U66+U67+U68+U166+U162",
    "is_calculated": True,
        "indent_level": 0
  },
  "U64 | o/w  subordinated debt": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U65 | o/w  hybrid borrowings / convertible bonds": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U66 | o/w  plain vanilla bonds": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U67 | o/w  discounted bills": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U68 | o/w  (finance or capital) lease": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U166 | o/w  current portion of LT debt": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U162 | o/w Bank overdraft": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "U183 | Lease liabilities non current IFRS16": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U176 | Advances & downpayments": {
    "aliases": ["Advances and downpayments", "Customer advances", "Advances from customers", "Advance payments from customers", "Downpayments from customers", "Down payments received", "Advance receipts from customers", "Deferred revenue (when labeled as advances)", "Unearned revenue (customer advances)", "Contract liabilities – advances", "Advances received for orders", "Advances received for goods and services", "Prepayments received from customers", "Advances from dealers/distributors", "Advance from related parties (if operating)", "Other advances received", "Trade advances received", "Advances and deposits received", "Security deposits received (short‑term, operating)", "Short‑term advances from customers", "Short‑term downpayments from customers"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

 "U75 | Accrued and other current liabilities": {
    "aliases": ["Accrued expenses", "Other current liabilities", "Derivative Liabilities", "loans from shareholders/related entities","Accrued and other current liabilities", "Other current liabilities and accruals", "Accrued liabilities", "Accrued expenses", "Accrued charges", "Accrued operating expenses", "Other current liabilities", "Miscellaneous current liabilities", "Sundry current liabilities", "Other payables and accrued expenses (if not strictly trade)", "Accruals and deferred income (current)", "Accrued payroll and related liabilities", "Accrued employee benefits (current portion)", "Accrued interest (current, non‑debt classification)", "Accrued taxes (non‑income tax, e.g., VAT, GST)", "Accrued utilities, rent, etc.", "Accrued costs and other current liabilities", "Other accrued liabilities", "Other short‑term liabilities", "Current accrued and other liabilities", "Current accrued liabilities"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U72 | Provisions for current liabilities": {
    "aliases": ["Provisions for current liabilities", "Current provisions", "Short‑term provisions", "Provisions (current portion)", "Current portion of provisions", "Provisions for warranties (current)", "Provision for warranty obligations – current", "Provisions for restructuring (current portion)", "Restructuring provision – current", "Provisions for legal claims (current portion)", "Legal provision – current", "Provisions for onerous contracts (current portion)", "Onerous contract provision – current", "Provision for environmental liabilities (current portion)", "Provision for employee benefits (current portion)", "Current provisions and contingent liabilities (when grouped)", "Provisions for risks and charges – current", "Short‑term provisions for risks and charges", "Other current provisions"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U74 | o/w provisions for guarantees granted": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U73 | o/w provisions for specific risks": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "U71 | Trade Accounts payable": {
    "aliases": ["Trade Creditors", "Accounts Payables","Trade accounts payable", "Trade payables", "Accounts payable – trade", "Trade creditors", "Sundry creditors (trade)", "Suppliers' payables", "Payables to suppliers", "Trade and other payables (trade portion)", "Bills payable (trade)", "Notes payable to suppliers", "Trade accounts and notes payable", "Trade accounts payable and accrued trade expenses", "Trade payables and accrued expenses (where clearly trade‑related)", "Commercial payables", "Trade liabilities"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U180 | Other payables": {
    "aliases": ["Other payables", "Non‑trade payables", "Sundry creditors (non‑trade)", "Other creditors", "Other current payables", "Miscellaneous payables", "Payables to employees (non‑wage accruals)", "Payables to related parties (non‑trade)", "Payables to group companies (non‑trade)", "Payables to directors/shareholders (non‑trade)", "Payables for taxes (excluding income tax if separately disclosed)", "VAT/GST payables (when grouped in other payables)", "Social security and pension payables", "Statutory dues payable", "Other liabilities – current (non‑trade)", "Other operating payables (non‑trade)", "Other short‑term payables", "Other current obligations (non‑trade)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

  "U70 | Current income tax liabilities": {
    "aliases": ["Current income tax liabilities", "Income taxes payable (current)", "Income tax payable", "Corporate income tax payable", "Current tax liabilities", "Current income tax payables", "Current tax payable", "Taxes on income payable (current)", "Provision for income tax (current)", "Taxation payable (income tax – current)", "Income tax due within one year", "Short‑term income tax liabilities", "Current liabilities – income tax", "Current income tax obligations"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "U177 | Others financial liabilities & FV of financial instruments": {
    "aliases": ["Other financial liabilities (current)", "Other current financial liabilities", "Other financial liabilities – short‑term", "Financial liabilities at fair value (current)", "Financial instruments at fair value (liabilities, current)", "Derivative financial liabilities (current)", "Derivative instruments – liabilities (current)", "Derivative liabilities – current", "Fair value of financial instruments (liabilities, current)", "Fair value of derivatives – current liabilities", "Financial liabilities measured at fair value (current)", "Other current financial obligations", "Other current interest‑bearing financial liabilities (non‑borrowings)", "Current liabilities – hedging instruments", "Liabilities arising from hedging instruments (current)", "Liabilities from financial guarantees (current)", "Contingent consideration (current, financial liability)", "Other current financial instruments (liabilities)", "Other short‑term financial liabilities", "Miscellaneous current financial liabilities"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U62init | CURRENT LIABILITIES pre-IFRS16": {
    "aliases": [],
    "calculation": "U63+U176+U75+U72+U71+U180+U70+U177-U183",
    "is_calculated": True,
        "indent_level": 0
  },

        "U62ifrs16 | CURRENT LIABILITIES": {
    "aliases": [],
    "calculation": "U63+U176+U75+U72+U71+U180+U70+U177",
    "is_calculated": True,
        "indent_level": 0
  },

  "U69 | Liabilities classified as held for sale": {
    "aliases": ["Liabilities classified as held for sale", "Liabilities of disposal groups classified as held for sale", "Liabilities associated with assets held for sale", "Liabilities of assets held for sale", "Liabilities related to disposal group held for sale", "Liabilities held for sale", "Held‑for‑sale liabilities", "Liabilities of discontinued operations (held for sale)", "Liabilities associated with discontinued operations (held for sale)", "Liabilities of disposal group", "Disposal group liabilities – held for sale", "Non‑current liabilities classified as held for sale (if specified)", "Current liabilities classified as held for sale (if specified)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "U78init | TOTAL LIABILITIES and EQUITIES IFRS16": {
    "aliases": [],
    "calculation": "U52INIT+U62INIT+U161",
    "is_calculated": True,
        "indent_level": 0
  },

    "U78 | TOTAL LIABILITIES and EQUITIES": {
    "aliases": [],
    "calculation": "U52IFRS+U62IFRS+U161",
    "is_calculated": True,
        "indent_level": 0
  },

    "DMLTB1 | Dette MLT > 1 an": {
    "aliases": [],
    "calculation": "",
    "is_calculated": False,
        "indent_level": 0
  },

    "DMLTB1 | Dette MLT > 1 an IFRS16": {
    "aliases": [],
    "calculation": "",
    "is_calculated": False,
        "indent_level": 0
  },

}

# ---------------------------------------------------------------------------
# APAC Cash Flow Fields (ICF-prefix codes)
# Same aliases as US fields, different codes for APAC region
# ---------------------------------------------------------------------------

APAC_CASH_FLOW_FIELDS = {
  "ICF14 | Net income": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF21 | +/-Income linked to associated companies": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF22 | +/-Cancellation of dividends (received from /paid to) unconsolidated companies.": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF23 | +/-Depreciation Amortization and Depletion": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF24 | +/-Amortization of Intangible Assets": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF25 | +/-Gain (loss) on sale of assets": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF26 | +/-Cost of financial debt, net": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF27 | +/-Tax (inc. Deffered tax, tax credit)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF28 | +/-Stock-based compensation expense": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF51 | +/- Litigation, fines": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF29 | +/- Others": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF52 | Cash-flow before cash interests and change in WCR": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF53 | Gross interests (cash)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "ICF53ifrs16 | lease interest IFRS16)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF01a | Cash-flow before change in WCR (FFO)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF15ifrs16 | EBITDA (published) IFRS": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF31 | Income tax (cash)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF51bis | + Litigation, fines": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF32 | +/- Others": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF01b | Cash-flow before cash interests and change in WCR": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF54 | Gross interests (cash)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

    "ICF54ifrs16 | Gross interests (cash) IFRS16": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF56 | Cash-flow before change in WCR (FFO)": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF52bis | Cash-flow before cash interests and change in WCR": {
    "aliases": ["Cash flow from operations before interest and taxes"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF53bis | - Gross interests (cash)": {
    "aliases": ["Cash interest paid","Gross interest (cash)", "Gross interests (cash)", "Interest (cash basis)", "Interest (cash)", "Interest paid and received (gross)", "Interest paid (cash)", "Interest received (cash)", "Interest expense (cash paid)", "Interest income (cash received)", "Cash interest expense", "Cash interest income", "Cash interest flows", "Finance costs (cash)", "Finance income (cash)", "Cash finance costs", "Cash finance income", "Interest on borrowings paid (cash)", "Interest on loans paid (cash)", "Interest on bonds paid (cash)", "Interest on debt paid (cash)", "Interest on bank loans paid (cash)", "Interest received on deposits (cash)", "Interest received on investments (cash)", "Interest received on loans granted (cash)", "Net interest (cash basis)", "Interest and similar charges (cash)", "Interest and similar income (cash)", "Cash flows from interest (gross)", "Cash interest paid and received"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF54ter | Lease interest IFRS16": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    
  "ICF01 | Cash-flow before change in WCR (FFO)": {
    "aliases": ["Funds from operations"],
    "calculation": "ICF52-ICF53-ICF54",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF02 | +/- Change in WCR": {
    "aliases": [],
    "calculation": "ICF45+ICF46+ICF47+ICF48+ICF49",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF45 | Decrease (increase) in inventories": {
    "aliases": ["(Increase) / decrease in inventory", "(Increase) / decrease in stock", "Change in inventories", "Movement in goods and materials", "Change in raw materials, work in progress and finished goods"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF46 | Decrease (increase) in trade receivables": {
    "aliases": ["(Increase) / decrease in trade receivables", "(Increase) / decrease in accounts receivable", "Movement in debtors", "Change in customers receivables", "Change in trade and other receivables", "(Increase) / decrease in bills receivable"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF47 | Increase (decrease) in trade payables": {
    "aliases": ["(Increase) / decrease in trade payables", "Change in accounts payable", "Movement in creditors", "Change in suppliers", "Change in trade and other payables", "(Increase) / decrease in bills payable"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF48 | Changes in other assets and liabilities": {
    "aliases": ["Change in deferred taxes/liabilities", "other changes","Change in other receivables", "Change in non‑trade receivables", "Change in prepayments", "Change in advances", "Change in tax receivables (income tax, VAT, GST)", "Change in sundry debtors (non‑trade)","Change in other payables", "Change in accrued liabilities", "Change in accruals", "Change in non‑trade creditors", "Change in tax payables (income tax, VAT, GST)", "Change in employee benefit payables", "Change in sundry creditors"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF49 | Others": {
    "aliases": ["Cash taxes", "cash tax payments","Income tax paid", "Corporate tax paid", "Taxes on income paid", "Taxation paid", "Payment of income taxes", "Net income tax paid (including refunds)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF03 | Operating cash flow": {
    "aliases": ["Cash flow from operations"],
    "calculation": "ICF01+ICF02",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF04 | CAPEX": {
    "aliases": [],
    "calculation": "ICF34+ICF35+ICF36",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF34 | (Purchases) sales of property, plant and equipment": {
    "aliases": ["Additions to tangible/intangible assets", "Capital Expenditure","Purchases and sales of property, plant and equipment", "Purchases and disposals of property, plant and equipment", "Purchases/(sales) of property, plant and equipment", "Acquisitions and disposals of property, plant and equipment", "Additions and disposals of property, plant and equipment", "Purchase and sale of fixed assets", "Purchases and sales of fixed assets", "Purchase/(sale) of fixed assets", "Acquisition and disposal of fixed assets", "Capital expenditure and disposals of PPE", "Investments in property, plant and equipment", "Investments in and sales of fixed assets", "Purchase/(disposal) of tangible fixed assets", "Movements in property, plant and equipment (purchases and disposals)", "Movements in fixed assets (purchases and sales)", "Capital work‑in‑progress (CWIP)", "Construction in progress (CIP)", "Assets under construction", "Work in progress – capital projects", "Projects under development", "Purchase of fixed assets", "Acquisition of property, plant and equipment", "Capital expenditure for PPE", "Additions to fixed assets", "Purchase of tangible fixed assets", "Acquisition of intangible assets", "Capitalization of development costs", "Purchase of software", "Purchase of licenses", "Purchase of patents, trademarks", "Investments in intangible assets", "Acquisition of investment property", "Purchase of rental property", "Purchase of real estate held for investment", "Investments in real estate"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF35 | Proceeds from sales of property, plant and equipment": {
    "aliases": ["Asset Sales", "Sales of PPE", "Divestment","Proceeds from sale of property, plant and equipment", "Proceeds from sales of property, plant and equipment", "Proceeds from disposal of property, plant and equipment", "Cash proceeds from sale of property, plant and equipment", "Cash received from sale of property, plant and equipment", "Cash received from disposal of property, plant and equipment", "Sale of property, plant and equipment (proceeds)", "Sale of property, plant and equipment (cash inflow)", "Disposal of property, plant and equipment (proceeds)", "Disposal of property, plant and equipment (cash inflow)", "Proceeds from sale of fixed assets", "Proceeds from disposal of fixed assets", "Cash received on sale of fixed assets", "Cash received on disposal of fixed assets", "Proceeds from sale of tangible fixed assets", "Proceeds from disposal of tangible fixed assets", "Proceeds from sale of PPE", "PPE disposals – cash received", "Proceeds from asset disposals (PPE)", "Proceeds from sale of property and equipment", "Sale of fixed assets", "Disposal of property, plant and equipment", "Proceeds from sale of tangible assets", "Proceeds from disposal of PPE", "Sale of investment property", "Disposal of investment property", "Proceeds from sale of real estate investments", "Sale of intangible assets", "Disposal of intangible assets", "Proceeds from sale of software / licenses", "Proceeds from disposal of intangibles"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF36 | +/- Others": {
    "aliases": ["Gain/loss from sales in subsidiaries", "other assets", "investments etc.","Other investing cash flows", "Other cash flows from investing activities", "Other cash movements from investing activities", "Other investing activities", "Other investment‑related cash flows", "Miscellaneous investing cash flows", "Miscellaneous investing activities", "Other investment cash movements", "Other investing inflows/(outflows)", "Other investing items", "Other investing transactions", "Other cash flows – investing section", "Other investing cash inflows/(outflows)", "Other investing operations", "Other investment operations", "Proceeds from sale of investments", "Redemption of investments", "Sale of marketable securities", "Proceeds from disposal of equity investments", "Proceeds from disposal of debt securities", "Purchase of investments", "Acquisition of equity investments", "Acquisition of debt securities", "Purchase of marketable securities", "Purchase of long‑term investments", "Purchase of short‑term investments", "Investment in bonds", "Purchase of financial assets"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF05 | Recurring Free Cash-Flow": {
    "aliases": [],
    "calculation": "ICF03+ICF04",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF06 | +/- Acquisitions net of disposals": {
    "aliases": [],
    "calculation": "ICF17+ICF18",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF17 | Acquisitions": {
    "aliases": ["Acquisition of subsidiaries", "Purchase of stake/shares in Subsidiaries/Other Entities","Acquisitions", "Acquisition of subsidiaries", "Acquisition of subsidiary", "Acquisition of businesses", "Acquisition of business units", "Acquisition of operations", "Business combinations – cash consideration", "Purchase of subsidiaries", "Purchase of subsidiary undertakings", "Purchase of business operations", "Investment in subsidiaries (cash paid)", "Investment in associates and joint ventures (cash paid)", "Acquisition of affiliates", "Acquisition of equity interests in subsidiaries/affiliates", "Cash paid for acquisitions", "Cash consideration for acquisitions", "Cash paid for business combinations", "Acquisitions (net of cash acquired)", "Net cash outflow on acquisition of subsidiaries", "Net cash outflow on acquisition of businesses"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF18 | Disposals": {
    "aliases": ["Divestment of stake/shares in Subsidiaries/Other Entities","Disposals", "Disposal of subsidiaries", "Disposal of subsidiary", "Disposal of businesses", "Disposal of business units", "Disposal of operations", "Sale of subsidiaries", "Sale of subsidiary undertakings", "Sale of business operations", "Disposal of affiliates", "Disposal of equity interests in subsidiaries/affiliates", "Cash received from disposals", "Cash proceeds from disposals", "Cash received from sale of subsidiaries", "Cash received from sale of businesses", "Proceeds from disposal of subsidiaries", "Proceeds from disposal of businesses", "Net cash inflow on disposal of subsidiaries", "Net cash inflow on disposal of businesses", "Disposals (net of cash disposed)", "Proceeds from disposal of controlled entities", "Divestment proceeds"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF07 | Dividend paid": {
    "aliases": [],
    "calculation": "ICF37+ICF38+ICF55",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF37 | Dividends paid to non-controlling interests": {
    "aliases": ["Dividend paid to minorities","Dividends paid to non‑controlling interests", "Dividends paid to non‑controlling shareholders", "Dividends paid to minority interests", "Dividends paid to minority shareholders", "Cash dividends paid to non‑controlling interests", "Cash dividends paid to minority interests", "Dividend payments to non‑controlling interests", "Dividend payments to minority shareholders", "Distribution to non‑controlling interests", "Distribution to minority interests", "Cash distributions to non‑controlling interests", "Cash distributions to minority shareholders", "Dividends to non‑controlling interests", "Dividends to minority interests"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF38 | Dividends paid to owners of the parent company": {
    "aliases": ["Equity Dividends", "Payments of divident to equity shareholders","Dividends paid to owners of the parent company", "Dividends paid to owners of the parent", "Dividends paid to shareholders of the parent", "Dividends paid to equity holders of the parent", "Dividends paid to ordinary shareholders", "Dividends paid to common shareholders", "Dividends paid to preference shareholders", "Cash dividends paid to shareholders", "Cash dividends paid to owners of the parent", "Cash dividends paid to equity holders", "Dividend payments to shareholders", "Dividend payments to owners of the parent", "Dividends paid (to shareholders)", "Dividends paid (to owners)", "Cash distributions to shareholders", "Cash distributions to equity holders", "Cash distributions to owners of the parent", "Equity dividends paid", "Dividends to shareholders of the company"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1   
  },
  "ICF55 | + Dividends from affiliates": {
    "aliases": ["Dividends from affiliates", "Dividends received from affiliates", "Dividends received from associated companies", "Dividends received from associates", "Dividends received from joint ventures", "Dividends received from subsidiaries", "Dividends from associates and joint ventures", "Dividends from equity‑accounted investees", "Dividend income from affiliates", "Dividend income from associates", "Dividend income from joint ventures", "Dividend income from investments in affiliates", "Cash dividends received from affiliates", "Cash dividends received from associates", "Cash dividends received from joint ventures", "Dividends from investments in associates/JV", "Dividends from investments in subsidiaries (when labelled as affiliates)", "Dividends received from related parties (affiliates)", "Dividend income received", "Dividends from subsidiaries / associates", "Dividends on investments received"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF08 | +/- Change in Capital": {
    "aliases": [],
    "calculation": "ICF39+ICF40",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF39 | Capital inc. (dec.) - owners of the parent company": {
    "aliases": ["Issuance of common/preferred shares", "Equity issuance","Capital increase/(decrease) – owners of the parent company", "Capital increase/(decrease) – owners of the parent", "Capital increases and reductions – shareholders of the parent", "Share capital increase/(decrease) – parent", "Share capital transactions – owners of the parent", "Changes in share capital – owners of the parent", "Changes in equity – owners of the parent (capital transactions)", "Issue and redemption of share capital – owners of the parent", "Issue and cancellation of shares – owners of the parent", "Equity contributions from owners of the parent", "Equity withdrawals by owners of the parent", "Capital contributions from shareholders of the parent", "Capital repayments to shareholders of the parent", "Proceeds from / repayment of equity capital – owners of the parent", "Proceeds from issue / buyback of shares – parent company", "Share buyback and issuance – parent company", "Movements in share capital – parent company (cash)", "Capital transactions with owners of the parent", "Transactions with owners of the parent in their capacity as owners (cash)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF40 | Capital inc. (dec.) - non-controlling interests": {
    "aliases": ["Purchase of minority shares", "Inc/dec in minority shareholding","Capital increase/(decrease) – non‑controlling interests", "Capital increases and reductions – non‑controlling interests", "Capital increase/(decrease) – minority interests", "Share capital transactions – non‑controlling interests", "Changes in share capital – non‑controlling interests", "Changes in equity – non‑controlling interests (capital transactions)", "Equity contributions from non‑controlling interests", "Equity contributions from minority interests", "Capital contributions from minority shareholders", "Capital injections by non‑controlling shareholders", "Capital repayments to non‑controlling shareholders", "Capital repayments to minority interests", "Proceeds from capital contributions – non‑controlling interests", "Payments for capital reductions – non‑controlling interests", "Transactions with non‑controlling interests (equity, cash)", "Ownership changes with non‑controlling interests (cash component)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF09 | +/- Change in Debt": {
    "aliases": [],
    "calculation": "ICF19+ICF20+ICF41+ICF42",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF19 | Proceeds from the issuance of debt": {
    "aliases": ["Proceeds from bank debt/bonds", "lease liabilities other debt","Proceeds from the issuance of debt", "Proceeds from issuance of debt", "Proceeds from issue of debt", "Proceeds from issuance of bonds", "Proceeds from issuance of debentures", "Proceeds from issuance of notes", "Proceeds from issuance of loan notes", "Proceeds from new borrowings", "Proceeds from long‑term borrowings", "Proceeds from short‑term borrowings", "Increase in borrowings (cash inflow)", "New loans raised", "New bank loans raised", "Cash received from new debt", "Cash proceeds from issuance of debt", "Cash proceeds from issue of debt securities", "Cash inflows from borrowings", "Proceeds from bank loans", "Proceeds from long‑term loans", "Proceeds from short‑term loans", "Proceeds from financing loans"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF20 | Repayment of detbs": {
    "aliases": ["Repayments of bank debt/bonds", "lease liabilities other debt","Repayment of debts", "Repayments of debts", "Repayment of debt", "Repayment of borrowings", "Repayments of borrowings", "Repayment of loans", "Loan repayments", "Repayment of bank loans", "Repayment of bank borrowings", "Repayment of long‑term borrowings", "Repayment of short‑term borrowings", "Redemption of bonds", "Redemption of debentures", "Redemption of notes", "Redemption of loan notes", "Cash outflows for debt repayment", "Cash used to repay borrowings", "Cash used for loan repayments", "Reduction in borrowings (cash outflow)"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },

    "ICF20ifrs16 | o/w repayment of detbs IFRS16": {
    "aliases": ["Loans from parent compny/shareholders/subsidiaries"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },

  "ICF41 | Loan granted to subsidiaries/JV": {
    "aliases": ["Loans from parent compny/shareholders/subsidiaries","Loan granted to subsidiaries/JV", "Loans granted to subsidiaries and joint ventures", "Loans granted to subsidiaries", "Loans granted to joint ventures", "Loans to subsidiaries", "Loans to joint ventures", "Loans to group companies", "Loans to affiliated companies", "Loans to related parties (subsidiaries/JV)", "Intercompany loans granted", "Intercompany financing – loans granted", "Cash advances to subsidiaries/JV", "Cash advances to group entities", "Lending to subsidiaries/JV", "Loans and advances to subsidiaries/JV", "Loan outflows to subsidiaries/JV", "Cash outflows for loans to subsidiaries/JV"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF42 | (Loan repaid from subsidiaries/JV)": {
    "aliases": ["Loans to parent compny/shareholders/subsidiaries","Loan repaid from subsidiaries/JV", "Loans repaid by subsidiaries and joint ventures", "Loans repaid by subsidiaries", "Loans repaid by joint ventures", "Repayment of loans from subsidiaries/JV", "Repayment of loans by subsidiaries/JV", "Cash received from loan repayments by subsidiaries/JV", "Cash received from subsidiaries/JV – loan repayment", "Intercompany loans repaid", "Intercompany financing – loans repaid", "Loan repayments from group companies", "Loan repayments from affiliated companies", "Loan repayments from related parties (subsidiaries/JV)", "Loans and advances recovered from subsidiaries/JV", "Cash inflows from loans repaid by subsidiaries/JV"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 1
  },
  "ICF33 | +/- Change in perimeter": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF10 | +/- Others (Currency…)": {
    "aliases": ["FX changes", "currency","Others (currency)", "Other (currency effects)", "Other (foreign exchange)", "Other currency translation effects", "Other FX translation effects", "Other foreign exchange differences on cash", "Other currency differences on cash and cash equivalents", "Other FX impact on cash", "Other exchange rate effects on cash", "Effect of currency translation on cash (other)", "Miscellaneous currency effects", "Miscellaneous foreign exchange effects", "Other non‑cash currency adjustments", "Other foreign exchange adjustments on cash flows", "Other translation differences on cash and cash equivalents", "Other exchange differences on cash", "Other items (including currency effects)", "Other reconciliation items (currency)", "Other adjustments – currency / FX"],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF11 | +/- Change in Cash": {
    "aliases": [],
    "calculation": "ICF05+ICF06+ICF07+ICF08+ICF09+ICF33+ICF10",
    "is_calculated": True,
        "indent_level": 0
  },
  "ICF12 | +/- Operations non poursuivies": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF16 | TRESORERIE A L'OUVERTURE": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  },
  "ICF13 | TRESORERIE A LA CLOTURE": {
    "aliases": [],
    "calculation": "ICF11+ICF12+ICF16",
    "is_calculated": True,
        "indent_level": 0
  },
  "CAFE1 | CAF": {
    "aliases": [],
    "calculation": None,
    "is_calculated": False,
        "indent_level": 0
  }
}

# Map statement types to field dictionaries (US region - default)
FIELD_MAPPINGS = {
    "income_statement": INCOME_STATEMENT_FIELDS,
    "balance_sheet": BALANCE_SHEET_FIELDS,
    "cash_flow": CASH_FLOW_FIELDS,
}

FIELD_MAPPINGS_APAC = {
    "income_statement": APAC_INCOME_STATEMENT_FIELDS,
    "balance_sheet": APAC_BALANCE_SHEET_FIELDS,
    "cash_flow": APAC_CASH_FLOW_FIELDS,
}

# EMEA uses the same field codes as APAC (Q-prefix for income statement, U-prefix for balance sheet, ICF-prefix for cash flow)
FIELD_MAPPINGS_EMEA = {
    "income_statement": APAC_INCOME_STATEMENT_FIELDS,
    "balance_sheet": APAC_BALANCE_SHEET_FIELDS,
    "cash_flow": APAC_CASH_FLOW_FIELDS,
}


def get_field_mappings(region: str = "US") -> dict:
    """Return the field mappings dict for the given region.
    
    Args:
        region: Region code - "US", "APAC", or "EMEA"
        
    Returns:
        Dictionary of field mappings for the specified region
        
    Note:
        EMEA and APAC use the same field codes (Q/U/ICF prefixes)
        US uses different codes (I/B/L/ACF prefixes)
    """
    if region in ("APAC", "EMEA"):
        return FIELD_MAPPINGS_APAC  # EMEA uses same codes as APAC
    return FIELD_MAPPINGS


def get_calculated_with_fallback(region: str = "US", statement_type: str = "income_statement") -> list:
    """Return list of calculated fields that can fallback to direct matching.
    
    These are calculated fields (like SG&A) that might also appear directly in the annual report.
    If they appear directly, we should match them instead of calculating from sub-fields.
    
    Args:
        region: Region code - "US", "APAC", or "EMEA"
        statement_type: Statement type
        
    Returns:
        List of field labels that can fallback to direct matching
    """
    # US region fallback fields
    US_FALLBACK = {
        "income_statement": [
            "I4 | SG&A Expense",  # Can be matched directly if sub-fields (I48, I49, I53) don't exist
            "I35 | Provision for income taxes (benefit)",  # Can be matched directly if sub-fields don't exist
            
        ],
        "balance_sheet": [
                "B33 | Other debtors",
                "B14 | Inventories (net)",
                "B98 | Other current operating assets",
                "B41 | Current financial assets (incl. Derivatives) - Net",
                "B6 | Net fixed assets (property, plants and equipment, Net)",
                "B22 | Investment properties, net",
                "B3 | Other assets and Intangible - Net",
                "B47 | Non current financial assets (incl derivatives & hedging financial instruments)",
                "L22 | Short-term debt < 1 yr , including current maturities of LT debt",
                "L72 | Provisions for current liabilities",
                "L15 | Long-term debt, less current maturities",
                "L4 | Additional paid-in capital",
                "ACF19 | Net income"
            ],
        "cash_flow": [
                "ACF36 | Cash-flow before cash interests and change in WC",
                "ACF29 | EBITDA (published)",
                "ACF36bis | Cash-flow before cash interests and change in WC",
                "ACF04 | CAPEX",
                "ACF06 | +/- Acquisitions net of disposals"

        ]
    }
    
    # APAC/EMEA region fallback fields
    APAC_FALLBACK = {
        "income_statement": [
            "Q6 | SG&A Expense",  # Can be matched directly if sub-fields (Q48, Q49, Q50) don't exist
            "Q28 | Interest costs (gross)",  # Can be matched directly
        ],
        "balance_sheet": [
            "U10 | Other Intangible assets Net",
            "U2 | Tangible assets: Property, plant and equipment",
            "U6 | Investment properties",
            "U17 | Non current financial assets (incl derivatives & hedging financial instruments)",
            "U24 | Inventories (net)",
            "U31 | Other receivables",
            "U98 | Other current operating assets",
            "U36 | Current financial assets",
            "U39 | Net Cash and bank deposits (excluding overdrafts)",
            "U45 | Share premium",
            "U181 | Reserves",
            "U53init | o/w Borrowings / debt >1 yr (excluding current maturities of LT debt) pre-IFRS16",
            "U60 | Other non-current liabilities",
            "U63init | o/w Borrowings / debt < 1 yr (including current maturities of LT debt) pre-IFRS16",
            "U72 | Provisions for current liabilities",
        ],
        "cash_flow": [
                "ICF14 | Net income",
                "ICF52 | Cash-flow before cash interests and change in WCR",
                "ICF15ifrs16 | EBITDA (published) IFRS",
                "ICF02 | +/- Change in WCR",
                "ICF04 | CAPEX"                
        ]
    }
    
    # Select the appropriate fallback list based on region
    if region in ("APAC", "EMEA"):
        fallback_dict = APAC_FALLBACK
    else:
        fallback_dict = US_FALLBACK
    
    # Return the list for the specified statement type
    return fallback_dict.get(statement_type, [])
