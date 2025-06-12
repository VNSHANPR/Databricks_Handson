# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome Notebook
# MAGIC
# MAGIC &#9997;Presented by Yann COLINA, Brian MAK, Thomas BLONDELLE & Majeed MALIK&#9997;
# MAGIC
# MAGIC The aim of this notebook is to ensure a smooth and enjoyable onboarding in this Sandbox environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a quickstart cluster &#127950;
# MAGIC
# MAGIC 1. In the left sidebar, right-click the **Compute** button and open the link in a new window.
# MAGIC 1. On the Clusters page, click **Create with XS singlenode all-purpose cluster**.
# MAGIC 1. Select the latest ML Databricks runtime image: eg. **13.3 LTS ML**.
# MAGIC 1. Name the cluster for easy reference, for example **Quickstart CDI-number**.
# MAGIC 1. Under **Tags**, input **SBOX**.
# MAGIC 1. Click **Create Cluster** at the bottom left. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Attach the notebook to the cluster
# MAGIC
# MAGIC 1. Return to this notebook.
# MAGIC 2. Clone it and save it in your user folder and open it.
# MAGIC 1. In the notebook menu bar, click **Connect** at the top right and select the cluster created ("**Quickstart CDI-number**").
# MAGIC 1. Wait until the cluster changes to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate demo dataset
# MAGIC Run the two cells below

# COMMAND ----------

!pip3 install faker

from pyspark.sql.functions import *
from pyspark.sql.types import *
from faker import Faker

products = [
    "wizard for SAP Sales Cloud, trial edition",
    "user behavior mining for Spotlight by SAP",
    "transaction availability for remote sites",
    "traffic congestion management",
    "trading platform integration",
    "total delivered cost analytics",
    "the intercompany integration solution for SAP Business One",
    "taxpayer online services",
    "system management CLI for SAP Data Intelligence, cloud edition",
    "supplier relationship management procurement for public sector",
    "store clustering",
    "stock balancing management",
    "spend report",
    "software logistics toolset",
    "software development kit for SAP Plant Connectivity",
    "shop floor dispatching and monitoring tool for SAP ERP",
    "service taxation enhancements for Brazil",
    "service provider cockpit",
    "service parts management",
    "search and classification engine",
    "sailing analytics, cloud edition",
    "sailing analytics mobile app for iOS",
    "sailing analytics mobile app for Android",
    "retail promotion analyzer",
    "retail account origination for banking",
    "remote support platform for SAP Business One",
    "remote support component",
    "remote connector to SAP HANA for SAP Smart Business, executive edition",
    "regulatory reporting for SAP object event repository",
    "regulatory data viewer for SAP EHS Regulatory Content",
    "recycling administration",
    "real estate development lifecycle management",
    "public budget formulation",
    "project 'Sentinel,' cloud edition",
    "project 'Sentinel'",
    "project 'Price Preference'",
    "project 'Medical Research Insights'",
    "project 'Magellan'",
    "project 'Kyma'",
    "project 'Intelligent Loan Payment Handling'",
    "project 'Engineering Change Simulation'",
    "project 'Cloudifier'",
    "project 'CityApp' for iOS",
    "project 'CityApp' for Android",
    "project 'CityApp'",
    "project 'Apollo'",
    "process scheduling adapter for SAP Solution Manager",
    "process and data-exchange framework for utilities",
    "prepayment for utilities for SAP S/4HANA",
    "prepayment for utilities",
    "pre-determined pricing engine",
    "portal development kit for Microsoft .NET",
    "performance monitoring tool for SAP Business One",
    "pension return for the Netherlands",
    "overtake and undertake quantities billing for German energy utilities",
    "order engineering workbench",
    "operational banking extension for Commonwealth Bank of Australia",
    "openSAP for iOS",
    "online application submission management",
    "on-site event management component",
    "multitarget application archive builder",
    "multiresource scheduling",
    "multiresource scheduling",
    "mobile service for development and operations, Agentry component",
    "mobile service for development and operations",
    "mobile development kit extension for Visual Studio Code",
    "mobile development kit editor for SAP Cloud Platform Business Application Studio, on-premise edition",
    "mobile development kit editor",
    "mobile development kit client",
    "mobile applications on project 'Gateway'",
    "mobile app builder add-on for SAP Mobile Platform, consumer edition",
    "migration solution for SAP Business ByDesign feature pack",
    "meter operation service billing for German electricity and gas utilities",
    "message manager add-on for SAP SMS 365, enterprise service",
    "market process management for utilities for SAP S/4HANA",
    "market process management for utilities",
    "manager self-services add-on",
    "maintenance planner",
    "m@gic EDDY from Crossgate",
    "logistics add-on for import/export, localization for India",
    "logging of Web Dynpro for ABAP",
    "logging of Web Client UI",
    "logging of SAP Gateway",
    "logging of SAP GUI for Windows UI",
    "logging of SAP GUI for Windows",
    "logging of SAP BW access",
    "logging of RFC and Web services",
    "log collection tool for SAP Business One",
    "locomotive management",
    "localizations for banking services from SAP",
    "lifecycle management for SAP BusinessObjects Enterprise",
    "length extension for general ledger accounting for utilities and telecommunications",
    "length extension for general ledger accounting",
    "landscape verification for SAP Solution Manager",
    "intercompany data exchange for UK electric companies",
    "intercompany data exchange for Swiss electric companies",
    "intercompany data exchange for Swiss electric and gas utilities for SAP S/4HANA",
    "intercompany data exchange for Swiss electric and gas utilities",
    "intercompany data exchange for Slovakian electric companies",
    "intelligent traffic management",
    "integration readiness check for SAP S/4HANA Cloud",
    "integration option for Microsoft Office SharePoint software",
    "integration framework for small and midsize enterprises",
    "integration add-on for SAP ERP HCM and SAP SuccessFactors HXM Suite",
    "infrastructure service for SAP HANA Enterprise Cloud",
    "idea management for Questra",
    "heavy equipment management for SAP S/4HANA",
    "heavy equipment management for SAP CRM",
    "heavy equipment management",
    "gateway to SAP NetWeaver Mobile",
    "freight cost extension",
    "foundation on ABAP platform, version for SAP HANA",
    "financial foundation",
    "field masking for Web Dynpro for ABAP",
    "field masking for Web Client UI",
    "field masking for SAPUI5 and SAP Fiori",
    "field masking for SAP GUI",
    "field data capture for upstream allocations with SAP MII",
    "extended functions for U.S. federal agencies",
    "event enablement add-on for SAP Fieldglass solutions",
    "enterprise integration add-on for landscape transformation mobile apps",
    "enterprise agent for SAP StreamWork",
    "enhanced security issuance management",
    "electronic services invoicing for Brazil",
    "electronic ledger management for Turkey",
    "edge lifecycle management",
    "eFormsFactory from BSI",
    "e-billing for joint venture accounting",
    "dynamic test scripts",
    "distance determination service",
    "device connector for SAP HANA platform, Internet of Things",
    "development tools for SAP Work Zone",
    "development tools for SAP Cloud Platform Functions",
    "development tools for SAP Cloud Platform",
    "developer tools for SAP Cloud Platform Workflow",
    "data transfer and migration tool",
    "data synthesizer for machine learning",
    "data interface XML payload testing tool for SAP Business One",
    "data cleansing package for SAP BusinessObjects BI, Edge edition, version with data management",
    "crystalreports.com",
    "corporate payment connector for HSBC",
    "corporate payment connector for American Express",
    "core data svcs graphical modeler for SAP Cloud Platform Business Application Studio, on-prem. ed.",
    "core data services graphical modeler for SAP Cloud Platform Business Application Studio",
    "core add-on for SAP Workforce Management",
    "conversion, migration, and analysis tools",
    "conversion agent for SAP NetWeaver",
    "content bundle for SAP Data Intelligence",
    "connector for SAP Multi-Bank Connectivity",
    "composite application for sales order change management",
    "composite application for return merchandise authorization",
    "composite application for maintenance order processing",
    "composite application for creating project logs",
    "composite application for backorder processing",
    "component extension for SAP Environment, Health, and Safety Management",
    "commerce reporting add-on for SAP Mobile Platform, consumer edition",
    "code name 'Edison'",
    "citizen registration",
    "card management",
    "capital investment planning",
    "business process blueprint",
    "budget-based benefits selection",
    "banking services from SAP, localization for Russia",
    "banking services from SAP, localization for Latin America",
    "banking services from SAP, localization for C.I.S.",
    "banking services from SAP, localization for Brazil",
    "banking services from SAP, localization for Argentina",
    "banking services from SAP",
    "b-process",
    "automated store allocation",
    "attribute-based planning for supply network planning",
    "asset management for oil and gas",
    "asset accounting multiple calendar tool",
    "assessorial charge management",
    "application function library SDK for SAP HANA",
    "app builder",
    "analytical banking extension for banking services from SAP",
    "analytic blueprints from SAP",
    "analytic applications",
    "advanced simulation for supply network planning",
    "advanced data synchronization framework for mobile asset management",
    "add-on for vehicle stock management with SAP CRM",
    "add-on for mobile integration with SAP CRM",
    "adaptor for SAP Productivity Pak by ANCILE",
    "adapter for SAP HANA Blockchain service",
    "activePDF for Business Objects",
    "accelerated application delivery for SAP NetWeaver",
    "accelerated application delivery for OEMs",
    "Xcelsius Workgroup N-tier",
    "Xcelsius Standard N-tier",
    "Xcelsius Professional N-tier",
    "Xcelsius Present N-tier",
    "Xcelsius Engage N-tier",
    "Xcelsius Designer N-tier",
    "WorkConnect by SAP",
    "Websphere Application Server by IBM",
    "Web interface for discrete industries",
    "WTA Finals for iOS",
    "WTA Finals for Android",
    "Variant Configuration service",
    "Unit of Measure",
    "UI5 automation framework",
    "UI5 Inspector",
    "UI theme designer",
    "UI data protection masking for SAP S/4HANA",
    "UI data protection logging for SAP S/4HANA",
    "UI add-on for SAP Supplier Relationship Management",
    "UI add-on for SAP NetWeaver",
    "TwoGo by SAP mobile app",
    "TwoGo by SAP",
    "TrueRec by SAP for iOS",
    "TrueRec by SAP",
    "Track Shopping Carts",
    "Track Shipments",
    "Track Sales Order",
    "Track Purchase Order",
    "Time Processing",
    "Third-Party Ordering by Wipro",
    "TechniData Environmental Performance Management",
    "TechniData Compliance for Products",
    "TSC Product Sustainability Toolkit for SAP Product Stewardship Network",
    "System Landscape Information",
    "Syclo SMART Work Manager with Calibrations for Maximo",
    "Syclo SMART Work Manager for TRIRIGA",
    "Syclo SMART Work Manager for SAP",
    "Syclo SMART Work Manager for Maximo",
    "Syclo SMART Work Manager for Energy Delivery for SAP",
    "Syclo SMART Service Manager for SAP CRM",
    "Syclo SMART Schedule",
    "Syclo SMART Sales Manager for SAP",
    "Syclo SMART Rounds for SAP ERP",
    "Syclo SMART Inventory Manager for SAP ERP",
    "Syclo SMART Inventory Manager for Maximo",
    "Syclo SMART Auditor for Maximo",
    "Syclo SMART Approval Manager for SAP",
    "Syclo Agentry mobile device management",
    "Syclo Agentry analytics",
    "Sybase XTNDConnect PC",
    "Sybase Software Asset Management",
    "Sybase SDK, DBLib Kerberos Authentication Option",
    "Sybase SDK",
    "Sybase Replication Server, real-time loading edition",
    "Sybase Replication Server, heterogeneous edition",
    "Sybase Replication Server software",
    "Sybase Replication Agent, option for IBM DB2",
    "Sybase Replication Agent",
    "Sybase RemoteWare",
    "Sybase RFID Anywhere",
    "Sybase PowerDesigner software",
    "Sybase Panopticon",
    "Sybase Open Switch",
    "Sybase Mobiliser Platform",
    "Sybase Mobile Workflow for SAP Business Suite",
    "Sybase Mobile Sales for SAP CRM",
    "Sybase MainframeConnect",
    "Sybase M-Business Anywhere",
    "Sybase Liquidity Management Suite",
    "Sybase InfoMaker application",
    "Sybase Industry Warehouse Studio",
    "Sybase HIPAA Transaction Manager",
    "Sybase HIPAA Accelerator",
    "Sybase Fuzzy Logix",
    "Sybase Event Stream Processor",
    "Sybase EnterpriseConnect Data Access",
    "Sybase Enterprise Connect Data Access",
    "Sybase EDI Server",
    "Sybase ECRTP",
    "Sybase ECMap",
    "Sybase EC Gateway",
    "Sybase Data Integration Suite",
    "Sybase Corporate Online Banking",
    "Sybase Corporate Banking",
    "Sybase Answers Anywhere",
    "Sybase Aleri Streaming Platform",
    "Sybase Advantage Database Server",
    "Sybase Adaptive Server Enterprise Cluster Edition",
    "Sybase Adaptive Server Enterprise",
    "Sub-Daily Planning and Scheduling by Wipro",
    "Spotlight by SAP",
    "Spendwell, cloud edition",
    "Spendwell for iOS",
    "Spendwell for Windows 8",
    "Spendwell for Android",
    "Solheim Cup for iOS",
    "Solheim Cup for Android",
    "Solheim Cup",
    "Social MeetUp by SAP application",
    "Social MeetUp by SAP",
    "SmartOps Production Planning with Pattern Optimization",
    "SmartOps Adaptive Structural Forecasting",
    "Share Your SAP Story for iOS",
    "Service Ticket Intelligence",
    "SSPS Modeler Banking from IBM",
    "SQL Anywhere",
    "SMS builder add-on for SAP Mobile Platform, consumer edition",
    "SMART Service Manager for SAP CRM by Syclo",
    "SB50 Host Committee Volunteers for iOS",
    "SB50 Host Committee Volunteers for Windows phone",
    "SB50 Host Committee Volunteers for Android",
    "SAPUI5",
    "SAP xApp Product Definition",
    "SAP xApp Lean Planning and Operations",
    "SAP xApp Cost and Quotation Management",
    "SAP workspaces",
    "SAP waste and recycling solutions by PROLOGA",
    "SAP solution extensions by GK",
    "SAP marketplace connector",
    "SAP for Utilities, customer care and service component",
    "SAP for Oil & Gas, production and revenue accounting component",
    "SAP for Oil & Gas, offshore logistics management component",
    "SAP for Me for iOS",
    "SAP for Me for Android",
    "SAP enhancement package for SAP Supply Chain Management",
    "SAP enhancement package for SAP Supplier Relationship Management",
    "SAP enhancement package for SAP SCM, version for SAP HANA",
    "SAP enhancement package for SAP ERP, version for SAP HANA",
    "SAP enhancement package for SAP ERP",
    "SAP enhancement package for SAP CRM, version for SAP HANA",
    "SAP enhancement package for SAP CRM",
    "SAP digital payments add-on",
    "SAP customer journey manager add-on",
    "SAP cProject Suite",
    "SAP assurance and compliance software for SAP S/4HANA",
    "SAP assurance and compliance software",
    "SAP address directory, global edition",
    "SAP address directory for the United States, monthly version for Z4change",
    "SAP address directory for the United States, monthly version for Suitelink",
    "SAP address directory for the United States, monthly version for Residential Delivery Indicator",
    "SAP address directory for the United States, monthly version for Lacslink",
    "SAP address directory for the United States, monthly version for Diversified",
    "SAP address directory for the United States, monthly version for DPV",
    "SAP address directory for the United States",
    "SAP address directory for the United Kingdom",
    "SAP address directory for the Netherlands",
    "SAP address directory for the Czech Republic",
    "SAP address directory for Turkey",
    "SAP address directory for Taiwan",
    "SAP address directory for Switzerland",
    "SAP address directory for Sweden",
    "SAP address directory for Spain",
    "SAP address directory for South Korea",
    "SAP address directory for Slovakia",
    "SAP address directory for Singapore",
    "SAP address directory for Russia",
    "SAP address directory for Portugal",
    "SAP address directory for Poland",
    "SAP address directory for Norway",
    "SAP address directory for New Zealand",
    "SAP address directory for Mexico",
    "SAP address directory for Macau, China",
    "SAP address directory for Luxemburg",
    "SAP address directory for Lithuania",
    "SAP address directory for Latvia",
    "SAP address directory for Japan",
    "SAP address directory for Japan",
    "SAP address directory for Italy",
    "SAP address directory for Ireland",
    "SAP address directory for India",
    "SAP address directory for Hungary",
    "SAP address directory for Greece",
    "SAP address directory for Germany, enhanced edition",
    "SAP address directory for Germany",
    "SAP address directory for France",
    "SAP address directory for Finland",
    "SAP address directory for Estonia",
    "SAP address directory for Denmark",
    "SAP address directory for Cyprus",
    "SAP address directory for China",
    "SAP address directory for China",
    "SAP address directory for Canada",
    "SAP address directory for Bulgaria",
    "SAP address directory for Brazil",
    "SAP address directory for Belgium",
    "SAP address directory for Austria",
    "SAP address directory for Australia",
    "SAP Yard Logistics for SAP S/4HANA",
    "SAP Yard Logistics",
    "SAP XBRL Publishing by UBmatrix",
    "SAP Workplaces for Insurance",
    "SAP Working Capital Analytics, DSO scope",
    "SAP Working Capital Analytics",
    "SAP Workforce Scheduling and Optimization by ClickSoftware",
    "SAP Workforce Performance Builder, producer option for OEMs",
    "SAP Workforce Performance Builder, producer option",
    "SAP Workforce Performance Builder, navigator option for OEMs",
    "SAP Workforce Performance Builder, navigator option",
    "SAP Workforce Performance Builder, manager option",
    "SAP Workforce Performance Builder, enterprise edition for OEMs",
    "SAP Workforce Performance Builder, enterprise edition",
    "SAP Workforce Performance Builder, desktop edition for OEMs, instant producer option",
    "SAP Workforce Performance Builder, desktop edition for OEMs",
    "SAP Workforce Performance Builder, desktop edition",
    "SAP Workforce Management",
    "SAP Workforce Forecasting and Scheduling by WorkForce Software",
    "SAP Workforce Deployment for Retail and Wholesale Distribution",
    "SAP WorkDeck for Windows tablets",
    "SAP WorkDeck",
    "SAP Work Zone for iOS",
    "SAP Work Zone for HR",
    "SAP Work Zone for Android",
    "SAP Work Zone",
    "SAP Work Manager, source code shipment",
    "SAP Work Manager for iOS",
    "SAP Work Manager for Windows Platform",
    "SAP Work Manager for Windows Mobile",
    "SAP Work Manager for TRIRIGA for Windows Mobile",
    "SAP Work Manager for TRIRIGA",
    "SAP Work Manager for Maximo, source code shipment",
    "SAP Work Manager for Maximo mobile app",
    "SAP Work Manager for Maximo",
    "SAP Work Manager for Android",
    "SAP Work Manager by Syclo",
    "SAP Work Manager",
    "SAP Work Efficiency Component",
    "SAP Web IDE, personal edition",
    "SAP Web IDE, hybrid app toolkit add-on",
    "SAP Web IDE, codeless toolkit",
    "SAP Web IDE plug-in for UI adaptation",
    "SAP Web IDE plug-in for SAP Manufacturing Integration and Intelligence",
    "SAP Web IDE plug-in for SAP Fiori launchpad extensions",
    "SAP Web IDE plug-in",
    "SAP Web IDE hybrid app toolkit add-on",
    "SAP Web Dispatcher",
    "SAP Web Channel Experience Management",
    "SAP Web Application Server for SAP S/4HANA",
    "SAP Web Analytics",
    "SAP Watch List Screening",
    "SAP Waste and Recycling, cloud edition by PROLOGA",
    "SAP Waste and Recycling",
    "SAP Warehouse Insights",
    "SAP Vora",
    "SAP Visual Information for Plants by NRX",
    "SAP Visual Enterprise",
    "SAP Visual Business",
    "SAP Vehicles Network",
    "SAP Vehicle Insights",
    "SAP Variant Configuration and Pricing",
    "SAP Utility Customer E-Services",
    "SAP Utilities Issue Reporter for iPhone",
    "SAP Utilities Customer Engagement, source code shipment",
    "SAP Utilities Customer Engagement, cloud edition",
    "SAP Utilities Customer Engagement for iOS",
    "SAP Utilities Customer Engagement for Android",
    "SAP Utilities Customer Engagement",
    "SAP User Experience Monitor for iOS",
    "SAP User Experience Monitor for Android",
    "SAP User Experience Management by Knoa, cloud edition",
    "SAP User Experience Management by Knoa",
    "SAP Upstream Operations Performance Analysis",
    "SAP Upstream Operations Management operational analysis",
    "SAP Upstream Operations Management",
    "SAP Upstream Field Activity Management by OIS",
    "SAP Upscale Commerce",
    "SAP Underwriting for Insurance",
    "SAP U.S. Payroll Tax Calculation by BSI",
    "SAP Tutor",
    "SAP Truck Tour for iOS",
    "SAP Truck Tour for Android",
    "SAP Truck Tour",
    "SAP Treasury and Risk Management, version for the United States",
    "SAP Travel Receipts Management by OpenText, option for OCR",
    "SAP Travel Receipts Management by OpenText",
    "SAP Travel Receipt Capture, source code shipment",
    "SAP Travel Receipt Capture for iPhone",
    "SAP Travel Receipt Capture for BlackBerry",
    "SAP Travel Receipt Capture for Android",
    "SAP Travel Receipt Capture",
    "SAP Travel Expense Report, source code shipment",
    "SAP Travel Expense Report for iPhone",
    "SAP Travel Expense Report for BlackBerry",
    "SAP Travel Expense Report for Android",
    "SAP Travel Expense Report",
    "SAP Travel Expense Approval, source code shipment",
    "SAP Travel Expense Approval for iPhone",
    "SAP Travel Expense Approval for BlackBerry",
    "SAP Travel Expense Approval",
    "SAP Transportation Resource Planning",
    "SAP Transportation Management, order to cash for container shipping liners for SAP S/4HANA",
    "SAP Transportation Management, order to cash for container shipping liners",
    "SAP Transportation Management, network and operations for container shipping liners for SAP S/4HANA",
    "SAP Transportation Management, network and operations for container shipping liners",
    "SAP Transportation Management, localization for Russia",
    "SAP Transportation Management, lead to agreement for container shipping liners for SAP S/4HANA",
    "SAP Transportation Management, lead to agreement for container shipping liners",
    "SAP Transportation Management integration with SAP Convergent Invoicing",
    "SAP Transportation Management for Panalpina",
    "SAP Transportation Management",
    "SAP Transport Tendering, source code shipment",
    "SAP Transport Tendering for iPhone",
    "SAP Transport Tendering",
    "SAP Transport Notification and Status, source code shipment",
    "SAP Transport Notification and Status for Android",
    "SAP Transport Notification and Status",
    "SAP Translation Hub",
    "SAP Transactionware General Merchandise",
    "SAP Transactionware Enterprise",
    "SAP Transactional Banking for SAP S/4HANA",
    "SAP Transaction Impact Monitor by CA",
    "SAP Trader's and Scheduler's Workbench",
    "SAP Trade Repository Reporting by Virtusa",
    "SAP Trade Promotion Planning, version for in-memory computing",
    "SAP Trade Promotion Optimization",
    "SAP Trade Promotion Effectiveness Analysis",
    "SAP Trade Management, version for SAP BW/4HANA",
    "SAP Trade Management",
    "SAP Total Margin Management",
    "SAP Timesheet, source code shipment",
    "SAP Timesheet for iPhone",
    "SAP Timesheet for BlackBerry",
    "SAP Timesheet",
    "SAP Time and Attendance Management by WorkForce Software",
    "SAP Time Recording",
    "SAP Time Management by Kronos",
    "SAP Text Analysis SDK",
    "SAP Text Analysis",
    "SAP Text Analysis",
    "SAP Test Execution Engine",
    "SAP Test Data Migration Server, extension for HCM",
    "SAP Test Data Migration Server, extension for BI and CRM",
    "SAP Test Data Migration Server, business process library extension",
    "SAP Test Data Migration Server",
    "SAP Test Acceleration and Optimization",
    "SAP Territory and Quota",
    "SAP Tennis applications back end",
    "SAP Tempo Box by OpenText, standard edition",
    "SAP Tempo Box by OpenText, premium edition",
    "SAP Technical Data Export Compliance by NextLabs",
    "SAP Teamcenter foundation by Siemens",
    "SAP Teamcenter by Siemens, gateway for enterprise applications",
    "SAP Teamcenter by Siemens integration for SolidWorks",
    "SAP Teamcenter by Siemens integration for Inventor",
    "SAP Teamcenter by Siemens integration for Creo",
    "SAP Teamcenter by Siemens integration for CATIA",
    "SAP Teamcenter by Siemens integration for AutoCAD",
    "SAP Team One",
    "SAP Tax Intelligence and Management by All Tax, TRABALHIST module",
    "SAP Tax Intelligence and Management by All Tax, RETIDOS and PIS/CONFINS module",
    "SAP Tax Intelligence and Management by All Tax, PIS/COFINS module",
    "SAP Tax Intelligence and Management by All Tax, IRPJ/CSLL module",
    "SAP Tax Intelligence and Management by All Tax, ICMS/IPI module",
    "SAP Tax Intelligence and Management by All Tax",
    "SAP Tax Declaration Framework for Brazil",
    "SAP Tax Classification and Reporting",
    "SAP Talent Visualization by Nakisa, viewing option",
    "SAP Talent Visualization by Nakisa",
    "SAP TDMS Manager, source code shipment",
    "SAP TDMS Manager for iOS",
    "SAP System Monitoring for iOS",
    "SAP System Monitoring for Android, source code shipment",
    "SAP System Monitoring for Android",
    "SAP Sybase RAP",
    "SAP Sybase PowerAMC",
    "SAP Sybase Mainframe Connect",
    "SAP Sybase IQ",
    "SAP Sustainability Performance Management",
    "SAP Supply Network Collaboration, UI add-on for purchase order collaboration",
    "SAP Supply Network Collaboration",
    "SAP Supply Chain Response Management by icon-scm",
    "SAP Supply Chain Performance Management",
    "SAP Supply Chain Management, enterprise services bundle",
    "SAP Supply Chain Management, add-on for transportation management",
    "SAP Supply Chain Management",
    "SAP Supply Chain Info Center",
    "SAP Supply Base Optimization",
    "SAP Supplier Relationship Management, localization for Israel",
    "SAP Supplier Relationship Management, localization for India",
    "SAP Supplier Relationship Management, enterprise services bundle",
    "SAP Supplier Relationship Management, add-on for procurement planning",
    "SAP Supplier Relationship Management for SAP ERP",
    "SAP Supplier Relationship Management",
    "SAP Supplier Lifecycle Management",
    "SAP Supplier InfoNet",
    "SAP SuccessFactors platform",
    "SAP SuccessFactors Workforce Planning",
    "SAP SuccessFactors Workforce Analytics",
    "SAP SuccessFactors Work-Life, Thrive Global add-on",
    "SAP SuccessFactors Work-Life for iOS",
    "SAP SuccessFactors Work-Life for Android",
    "SAP SuccessFactors Work-Life",
    "SAP SuccessFactors Visa and Permits Management",
    "SAP SuccessFactors Time Tracking",
    "SAP SuccessFactors Succession & Development",
    "SAP SuccessFactors Recruiting Posting",
    "SAP SuccessFactors Recruiting",
    "SAP SuccessFactors Performance & Goals",
    "SAP SuccessFactors People Central Hub",
    "SAP SuccessFactors People Analytics",
    "SAP SuccessFactors Onboarding, consumption edition",
    "SAP SuccessFactors Onboarding",
    "SAP SuccessFactors Mobile for iOS",
    "SAP SuccessFactors Mobile for Android",
    "SAP SuccessFactors Mobile",
    "SAP SuccessFactors Learning downloads",
    "SAP SuccessFactors Learning Marketplace",
    "SAP SuccessFactors Learning",
    "SAP SuccessFactors HXM Suite, integration for SAP ERP",
    "SAP SuccessFactors HXM Suite",
    "SAP SuccessFactors HXM Core",
    "SAP SuccessFactors Extended Enterprise Content Management by OpenText, cloud edition",
    "SAP SuccessFactors Extended Enterprise Content Management by OpenText",
    "SAP SuccessFactors Employee Central integration to SAP Business Suite",
    "SAP SuccessFactors Employee Central Service Center",
    "SAP SuccessFactors Employee Central Payroll, third-party data integration tool",
    "SAP SuccessFactors Employee Central Payroll front end",
    "SAP SuccessFactors Employee Central Payroll",
    "SAP SuccessFactors Employee Central",
    "SAP SuccessFactors Document Management Core by Open Text",
    "SAP SuccessFactors Compensation",
    "SAP Subscription Billing",
    "SAP Student Lifecycle Management",
    "SAP Student Activity Hub, on-premise edition",
    "SAP Student Activity Hub",
    "SAP Streaming Analytics",
    "SAP StreamWork, mobile version",
    "SAP StreamWork plug-in for Microsoft Outlook",
    "SAP StreamWork Feed Updates",
    "SAP StreamWork Brainstorming",
    "SAP StreamWork",
    "SAP Strategy Management",
    "SAP Strategic Workforce Planning, version for in-memory computing",
    "SAP Strategic Enterprise Management, add-on for financial SASAC reporting for China by Pansoft",
    "SAP Strategic Enterprise Management",
    "SAP StormRunner Load by Micro Focus",
    "SAP Stored Value Program",
    "SAP Store Device Control by GK",
    "SAP Staff Productivity Management for Healthcare",
    "SAP Sports One, cloud edition",
    "SAP Spend Performance Management",
    "SAP Spend Analysis OnDemand",
    "SAP Sourcing, integration with SAP ERP",
    "SAP Sourcing and SAP Contract Lifecycle Workflow",
    "SAP Sourcing and SAP Contract Lifecycle Management",
    "SAP Sourcing OnDemand",
    "SAP Solution Sales Configuration, on-premise edition, UI composer add-on",
    "SAP Solution Sales Configuration, on-premise edition",
    "SAP Solution Sales Configuration for SAP S/4HANA",
    "SAP Solution Manager, version for SAP HANA",
    "SAP Solution Manager, enterprise edition",
    "SAP Solution Manager adapter for SAP Quality Center by HPE",
    "SAP Solution Manager Dashboards for iOS",
    "SAP Solution Manager",
    "SAP Social Media Analytics by NetBase",
    "SAP Social Contact Intelligence for iPhone",
    "SAP Social Channels 365",
    "SAP Smart Meter Analytics",
    "SAP Smart Business, service discovery",
    "SAP Smart Business, executive edition for SAP Marketing",
    "SAP Smart Business, executive edition",
    "SAP Smart Business, core data services version",
    "SAP Smart Business, component for KPI modeling",
    "SAP Smart Business, cloud edition",
    "SAP Smart Business tile service for SAP Fiori launchpad",
    "SAP Smart Business service",
    "SAP Smart Business foundation component",
    "SAP Smart Business for transportation management",
    "SAP Smart Business for retail promotion execution",
    "SAP Smart Business for real estate, executive edition",
    "SAP Smart Business for financial close",
    "SAP Smart Business for extended warehouse management",
    "SAP Smart Business for event management",
    "SAP Smart Business for commercial project management",
    "SAP Smart Business for SAP solutions for GRC",
    "SAP Smart Business for SAP Transportation Management",
    "SAP Smart Business for SAP S/4HANA Finance",
    "SAP Smart Business for SAP PLM",
    "SAP Smart Business for SAP Information Lifecycle Management",
    "SAP Smart Business for SAP Global Trade Services",
    "SAP Smart Business for SAP Fashion Management",
    "SAP Smart Business for SAP Event Management and SAP Transportation Management cross-analytics",
    "SAP Smart Business for SAP Event Management and SAP Transportation Management 9.3 cross-analytics",
    "SAP Smart Business for SAP ERP",
    "SAP Smart Business for SAP Commodity Management",
    "SAP Smart Business for SAP CRM and SAP ERP cross-analytics",
    "SAP Smart Business for SAP CRM",
    "SAP Smart Business for SAP Business Suite foundation component",
    "SAP Smart Business for SAP Advanced Planning and Optimization",
    "SAP Smart Business",
    "SAP Single Sign-On",
    "SAP Signature Management by DocuSign, government compliance option for SAP SuccessFactors solutions",
    "SAP Signature Management by DocuSign, government compliance option for SAP Customer Experience",
    "SAP Signature Management by DocuSign, government compliance option for SAP Ariba solutions",
    "SAP Signature Management by DocuSign, enterprise edition for SAP SuccessFactors solutions",
    "SAP Signature Management by DocuSign, enterprise edition for SAP Customer Experience solutions",
    "SAP Signature Management by DocuSign, enterprise edition for SAP Ariba solutions",
    "SAP Signature Management by DocuSign, add-on for standards-based signature compliance",
    "SAP Signature Management by DocuSign, add-on for SAP Customer Experience solutions",
    "SAP Signature Management by DocuSign, add-on for Part 11 regulations",
    "SAP Signature Management by DocuSign",
    "SAP Shopper Experience, source code shipment",
    "SAP Shopper Experience, cloud edition",
    "SAP Shopper Experience for iOS",
    "SAP Shopper Experience for Android",
    "SAP Shopper Experience",
    "SAP Service Virtualization by Micro Focus",
    "SAP Service Level Optimization by SmartOps",
    "SAP Self-Service for Utilities, source code shipment",
    "SAP Self-Service for Utilities for iOS",
    "SAP Self-Service for Utilities for Android",
    "SAP Self-Service Accelerator for Utilities by SEW, cloud edition",
    "SAP Self-Service Accelerator for Utilities by SEW",
    "SAP Self-Billing Cockpit",
    "SAP Secondary Distribution for Oil & Gas",
    "SAP Search and Discovery",
    "SAP Screen Personas",
    "SAP Scouting for college sports",
    "SAP Scout One for iOS",
    "SAP Scheduling and Resource Management by ClickSoftware",
    "SAP Sanctioned-Party List, source code shipment",
    "SAP Sanctioned-Party List for iPhone",
    "SAP Sanctioned-Party List",
    "SAP Sales and Operations Planning rapid-deployment solution",
    "SAP Sales and Operations Planning OnDemand",
    "SAP Sales and Operations Planning",
    "SAP Sales and Operations Planning",
    "SAP Sales Rapid Mart for Oracle",
    "SAP Sales Rapid Mart",
    "SAP Sales Pipeline Simulator for Windows tablets",
    "SAP Sales Pipeline Simulator",
    "SAP Sales Order Notification, source code shipment",
    "SAP Sales Order Notification for iPhone",
    "SAP Sales Order Notification",
    "SAP Sales Manager, source code shipment",
    "SAP Sales Manager, add-on for integration with Agentry",
    "SAP Sales Manager integration with SAP ERP",
    "SAP Sales Manager integration with SAP CRM",
    "SAP Sales Manager for iOS",
    "SAP Sales Manager for Windows",
    "SAP Sales Manager for Android",
    "SAP Sales Manager",
    "SAP Sales Insights for Retail",
    "SAP Sales Express",
    "SAP Sales Diary, source code shipment",
    "SAP Sales Diary for iPad",
    "SAP Sales Diary",
    "SAP Sales Companion, source code shipment",
    "SAP Sales Companion for iPad",
    "SAP Sales Companion for Windows tablets",
    "SAP Sales Companion",
    "SAP Sales Cloud, starter edition",
    "SAP Sales Cloud, imaging intelligence add-on",
    "SAP Sales Cloud mobile app",
    "SAP Sales Analysis for Retail, edition for SAP HANA",
    "SAP Sales Analysis for Retail",
    "SAP Sailing Analytics",
    "SAP SRM, supplier collaboration engine add-on",
    "SAP SRM, add-on for bid security and expert bid evaluation",
    "SAP SQL Anywhere, cloud edition",
    "SAP SQL Anywhere, Cryptographic Interface Option",
    "SAP SQL Anywhere",
    "SAP SMS Firewall 365",
    "SAP SMS 365 Operator Dashboard for iOS",
    "SAP SMS 365 Operator Dashboard for Android",
    "SAP SMS 365 Dashboard for iOS SDK",
    "SAP SMS 365 Dashboard for Android SDK",
    "SAP SMS 365 Dashboard for Android",
    "SAP SMS 365 Dashboard",
    "SAP S/4HANA, supply chain integration add-on for SAP Integrated Business Planning",
    "SAP S/4HANA, bulk transportation extension for SAP Agricultural Contract Management",
    "SAP S/4HANA, add-on for mobile integration",
    "SAP S/4HANA integration with SAP Concur solutions",
    "SAP S/4HANA foundation",
    "SAP S/4HANA for waste and recycling by PROLOGA",
    "SAP S/4HANA for supplier quotation management",
    "SAP S/4HANA for subscription billing, sales and distribution option",
    "SAP S/4HANA for rights and royalty management by Vistex",
    "SAP S/4HANA for procurement planning",
    "SAP S/4HANA for financial products subledger",
    "SAP S/4HANA for customer management",
    "SAP S/4HANA for central procurement, integration with SAP ERP",
    "SAP S/4HANA for central procurement",
    "SAP S/4HANA for card management",
    "SAP S/4HANA for asset retirement obligation",
    "SAP S/4HANA for agreement profitability and negotiation by gicom",
    "SAP S/4HANA for accounting integration",
    "SAP S/4HANA Utilities master data updates option for Germany",
    "SAP S/4HANA Utilities for intelligent metering of German energy",
    "SAP S/4HANA Utilities for German billing enhancements",
    "SAP S/4HANA Utilities for EEG billing of German electricity",
    "SAP S/4HANA Utilities extensions for meter-to-cash processes by PROLOGA",
    "SAP S/4HANA Supply Chain for secondary distribution",
    "SAP S/4HANA Life Sciences for product data submission management",
    "SAP S/4HANA Insurance for reinsurance management",
    "SAP S/4HANA Finance, localization for Israel",
    "SAP S/4HANA Finance, localization extension for the Republic of Belarus by EPAM",
    "SAP S/4HANA Finance",
    "SAP S/4HANA Cloud integration with SAP Cloud for Customer",
    "SAP S/4HANA Cloud front end",
    "SAP S/4HANA Cloud for projects",
    "SAP S/4HANA Cloud for intelligent product selection",
    "SAP S/4HANA Cloud for enterprise contract assembly",
    "SAP S/4HANA Cloud for customer payments",
    "SAP S/4HANA Cloud for credit integration",
    "SAP S/4HANA Cloud for advanced financial closing",
    "SAP S/4HANA Cloud for Invoice Processing by OpenText",
    "SAP S/4HANA Cloud Suite Foundation",
    "SAP S/4HANA Cloud Data Enrichment",
    "SAP S/4HANA Cloud",
    "SAP S/4HANA Banking for payment centralization",
    "SAP S/4HANA Banking for complex loans",
    "SAP S/4HANA Automotive for manufacturing logistics",
    "SAP S/4HANA",
    "SAP Rural Sourcing Management for Android for Vegetable Oil Development Projects",
    "SAP Rural Sourcing Management for Android for CBI",
    "SAP Rural Sourcing Management for Android for Balmed",
    "SAP Rural Sourcing Management for Android",
    "SAP Rural Sourcing Management",
    "SAP Rounds Manager, Java source code shipment",
    "SAP Rounds Manager mobile app",
    "SAP Rounds Manager by Syclo",
    "SAP Rounds Manager",
    "SAP Roambi publishing client for SAP BusinessObjects BI 4.2",
    "SAP Roambi publishing client for SAP BusinessObjects BI 4.0",
    "SAP Roambi publishing client for JDBC",
    "SAP Roambi publishing client SDK",
    "SAP Roambi Flow iOS, demo version",
    "SAP Roambi Flow for iOS, customer enterprise edition",
    "SAP Roambi Flow for iOS, co-branded version",
    "SAP Roambi Flow for iOS with Good Technology",
    "SAP Roambi Flow for iOS",
    "SAP Roambi Enterprise Server by MeLLmo",
    "SAP Roambi Enterprise Server",
    "SAP Roambi Customs",
    "SAP Roambi Cloud",
    "SAP Roambi Analytics for iOS, demo version",
    "SAP Roambi Analytics for iOS, customer enterprise edition with Good Technology",
    "SAP Roambi Analytics for iOS, customer enterprise edition",
    "SAP Roambi Analytics for iOS, co-branded version",
    "SAP Roambi Analytics for iOS, Verizon customer edition",
    "SAP Roambi Analytics for iOS with Good Technology",
    "SAP Roambi Analytics for iOS",
    "SAP Roambi Analytics for Windows, internal customer enterprise ed., cross-platform v",
    "SAP Roambi Analytics for Windows, cross-platform version",
    "SAP Roambi Analytics for Windows, co-branded version, cross-platform version",
    "SAP Roambi Analytics for Google Chrome",
    "SAP Roambi Analytics for Android, customer enterprise ed., cross-platform version",
    "SAP Roambi Analytics for Android, co-branded version",
    "SAP River Rapid Development Environment plug-in",
    "SAP River Rapid Development Environment",
    "SAP River",
    "SAP Risk Management for SAP S/4HANA",
    "SAP Risk Management",
    "SAP Revenue Accounting and Reporting, integration with sales and distribution",
    "SAP Revenue Accounting and Reporting",
    "SAP Returns Authorization",
    "SAP Returnable Packaging Management",
    "SAP Retail Store Ops Manager, source code shipment",
    "SAP Retail Store Ops Manager for iOS",
    "SAP Retail Store Ops Associate, source code shipment",
    "SAP Retail Store Ops Associate for iPhone",
    "SAP Retail Execution, source code shipment",
    "SAP Retail Execution integration with SAP ERP",
    "SAP Retail Execution integration with SAP CRM",
    "SAP Retail Execution for mobile business object",
    "SAP Retail Execution for iOS",
    "SAP Retail Execution for Windows Mobile",
    "SAP Retail Execution for Android",
    "SAP Retail Execution",
    "SAP Resource and Portfolio Management",
    "SAP Resolve",
    "SAP Replication Server, real-time loading edition",
    "SAP Replication Server, option for Oracle",
    "SAP Replication Server, option for Microsoft SQL Server",
    "SAP Replication Server, option for IBM DB2",
    "SAP Replication Server, heterogeneous edition",
    "SAP Replication Server",
    "SAP Replication Manager, source code shipment",
    "SAP Replication Manager",
    "SAP Replication Agent, option for IBM DB2",
    "SAP Remote Access and Connectivity",
    "SAP Reinsurance Management",
    "SAP Regulation Management by Greenlight, cyber governance edition",
    "SAP Regulation Management by Greenlight",
    "SAP Recipe Development",
    "SAP Receivables Manager for iPad",
    "SAP Receivables Manager for Windows tablets",
    "SAP Receivables Manager",
    "SAP RealSpend, source code shipment",
    "SAP RealSpend mobile app",
    "SAP RealSpend for iPad",
    "SAP RealSpend for BlackBerry 10",
    "SAP RealSpend",
    "SAP Real-Time Situational Awareness",
    "SAP Real-Time Offer Management software",
    "SAP Real-Time Offer Management",
    "SAP Real-Time Communicator by GENBAND",
    "SAP Real Estate Management, localization for CEE countries",
    "SAP Real Estate Management, add-on for tenant relationship management",
    "SAP Real Estate Broker for iPad",
    "SAP Readiness Assessment for Defense & Security",
    "SAP Railcar Management tracking add-on",
    "SAP Railcar Management extension",
    "SAP Railcar Management Component",
    "SAP REACH Compliance",
    "SAP R/3 mobile services for utility companies, localization for India",
    "SAP R/3 add-on for public sector",
    "SAP R/3 add-on for mill products",
    "SAP R/3 add-on for information lifecycle management",
    "SAP R/3 add-on for high tech",
    "SAP R/3 add-on for engineering, construction, and operations",
    "SAP R/3 add-on for consumer products",
    "SAP R/3 add-on for banking",
    "SAP R/3 add-on for automotive",
    "SAP R/3 add-on for aerospace and defense",
    "SAP R/3 add-on for Dun & Bradstreet",
    "SAP R/3 Enterprise add-on for telecommunications",
    "SAP R/3 Enterprise add-on for oil and gas",
    "SAP R/3 Enterprise add-on for mining",
    "SAP R/3 Enterprise add-on for media",
    "SAP R/3 Enterprise add-on for information lifecycle management",
    "SAP R/3 Enterprise",
    "SAP R/3",
    "SAP Quotation and Underwriting for Insurance",
    "SAP Qualtrics Surveys for iOS",
    "SAP Qualtrics Surveys for Android",
    "SAP Qualtrics Follow-Up for iOS",
    "SAP Qualtrics Follow-Up for Android",
    "SAP Qualtrics Core XM",
    "SAP Quality Issue Management",
    "SAP Quality Center by Micro Focus, premier edition",
    "SAP Quality Center by Micro Focus, premier cloud edition",
    "SAP Quality Center by Micro Focus, enterprise edition",
    "SAP Quality Center by Micro Focus, cloud edition",
    "SAP Quality Center by HPE, unified functional testing module",
    "SAP Quality Center by HPE, service test module",
    "SAP Push Notification 365 SDK for PhoneGap",
    "SAP Push Notification 365",
    "SAP Purchasing Rapid Mart for Oracle",
    "SAP Purchasing Rapid Mart",
    "SAP Public Sector Collection and Disbursement",
    "SAP Public Budget Formulation",
    "SAP Promotion Merchandising Layout",
    "SAP Promotion Management, add-on for SAP Customer Activity Repository",
    "SAP Promotion Management",
    "SAP Project to Go mobile app",
    "SAP Project to Go",
    "SAP Project Intelligence Network for Construction",
    "SAP Project FileShare",
    "SAP Project Companion for Managers for iOS",
    "SAP Project Companion for Managers",
    "SAP Project Companion for Consultants for iOS",
    "SAP Project Companion 1.0.0 for iOS",
    "SAP Project Cockpit, on-demand version",
    "SAP Project Cockpit mobile app",
    "SAP Profitability and Performance Management",
    "SAP Profitability and Cost Management",
    "SAP Productivity Pak by ANCILE",
    "SAP Productivity Composer by ANCILE",
    "SAP Production and Revenue Accounting, add-on for reporting and processing",
    "SAP Production and Revenue Accounting",
    "SAP Production Sharing Accounting",
    "SAP Production Planning Rapid Mart",
    "SAP Product and Quotation Management for Insurance",
    "SAP Product Verification OnDemand",
    "SAP Product Stewardship Network",
    "SAP Product Safety Management OnDemand",
    "SAP Product Lifecycle Management for Insurance",
    "SAP Product Lifecycle Management for Digital Products",
    "SAP Product Lifecycle Costing, cloud edition",
    "SAP Product Lifecycle Costing",
    "SAP Product Content Hub",
    "SAP Product Configuration, on-premise edition",
    "SAP Procurement Intelligence",
    "SAP Process Performance Management by Software AG",
    "SAP Process Objects Builder",
    "SAP Process Mining by Celonis, cloud edition",
    "SAP Process Mining by Celonis",
    "SAP Process Integration, secure connectivity add-on",
    "SAP Process Integration, connectivity add-on",
    "SAP Process Integration, business-to-business add-on",
    "SAP Process Integration",
    "SAP Process Control, starter kit for financial reporting",
    "SAP Process Control for SAP S/4HANA",
    "SAP Process Control",
    "SAP Privacy Management by BigID",
    "SAP Privacy Governance",
    "SAP Pricing and Costing for Utilities",
    "SAP Pricing Profitability Analysis by Vendavo",
    "SAP Price and Margin Management by Vendavo",
    "SAP Price Optimization for Banking",
    "SAP Predictive service",
    "SAP Predictive Maintenance and Service, technical foundation",
    "SAP Predictive Maintenance and Service, on-premise edition",
    "SAP Predictive Maintenance and Service, cloud edition",
    "SAP Predictive Maintenance and Service, cloud edition",
    "SAP Predictive Maintenance and Service, add-on for utilities",
    "SAP Predictive Engineering Insights",
    "SAP Predictive Analytics, OEM edition",
    "SAP Predictive Analytics integrator",
    "SAP Predictive Analytics",
    "SAP Predictive Analysis",
    "SAP Precision Marketing",
    "SAP PowerDesigner",
    "SAP PowerBuilder",
    "SAP PowerAMC",
    "SAP Postalsoft, business edition",
    "SAP Postalsoft Printform",
    "SAP Postalsoft Presort",
    "SAP Postalsoft Match Consolidate",
    "SAP Postalsoft Label Studio",
    "SAP Postalsoft Global Match"
]

Faker.seed(0)
fake = Faker()

current_user = spark.sql("select current_user()").collect()[0].asDict()["current_user()"].replace("@sap.com", "").replace(".", "_")
data_area = "sbox"
storage_account = "coredatalakesandbox"
adls_root = f"abfss://{data_area}@{storage_account}.dfs.core.windows.net/store/{current_user}"

spark.sql("CREATE DATABASE IF NOT EXISTS sbox")

from datetime import datetime, timedelta

opportunities_schema = StructType([
  StructField("opp_id", IntegerType(), False),
  StructField("contact_person", StringType(), False),
  StructField("dist_channel", StringType(), False),
  StructField("status", StringType(), False),
  StructField("created_at", DateType(), False),
  StructField("deal_size_keur", IntegerType(), False),
  StructField("logical_product", StringType(), False),
  StructField("customer", StringType(), False),
])

opp_status = ["won", "discontinued", "in progress"]
distribution_channels = ["Retail", "Wholesale", "Sales reps", "Direct marketing", "Telemarketing", "Internet", "International"]
customers = [fake.unique.company() for _ in range(1000)]

opportunities_data = []
opp_id = 0
begin_ts, end_ts = datetime(2023, 1, 1), datetime(2023, 3, 1)

for customer in customers:
  contact_person = fake.first_name() + " " + fake.last_name()
  for opp_id in [fake.unique.random_int(0, 10000000) for _ in range(3)]:
    opportunities_data.append((
                              opp_id,
                              contact_person,
                              fake.random_choices(elements=distribution_channels, length=1)[0],
                              fake.random_choices(elements=opp_status, length=1)[0],
                              fake.date_between(begin_ts, end_ts),
                              fake.random_int(1000, int(1e6)),
                              fake.random_choices(elements=products, length=1)[0],
                              customer
                              ))
    opp_id += 1
opportunities_df = spark.createDataFrame(data=opportunities_data, schema=opportunities_schema)
opportunities_df.write.mode("overwrite").parquet(f"{adls_root}/opportunities")
# .saveAsTable(f"{data_area}.{current_user}_opportunities")


clickstream_schema = StructType([
  StructField("click_id", IntegerType(), False),
  StructField("timestamp", TimestampType(), False),
  StructField("session_id", IntegerType(), False),
  StructField("customer", StringType(), False),
  StructField("client_ip", StringType(), False),
  StructField("server_ip", StringType(), False),
  StructField("user_agent", StringType(), False),
  StructField("page_id", IntegerType(), False),
  StructField("product", StringType(), False),
  StructField("referring_page_id", IntegerType(), False),
  StructField("browsing_time_per_page", IntegerType(), False),
  StructField("num_errors", IntegerType(), False),
  StructField("kbytes_downloaded", IntegerType(), False),
])

start_ts, end_ts = datetime(2023, 1, 1), datetime(2023, 3, 1)
current_ts = start_ts

product_page_mapping_data = {p: fake.unique.random_int(0, 100000) for p in products}
customer_session_mapping_data = {c: fake.unique.random_int(1, 10000000) for c in customers}

click_id = 0
clickstream_data = []

while current_ts < end_ts:
    opp_sample = fake.random_choices(opportunities_data)
    for cust, prod, status in [(opp[7], opp[6], opp[3]) for opp in opp_sample]:
      if status == "won" and fake.random_int(0, 100) < 90:
        duration = fake.random_int(500, 1000)
      else:
        duration = fake.random_int(0, 240)
      clickstream_data.append((
        click_id,
        fake.date_time_between_dates(datetime_start=current_ts, datetime_end=current_ts + timedelta(days=1)),
        customer_session_mapping_data[cust],
        cust,
        fake.ipv4(),
        fake.ipv4(),
        fake.chrome(),
        product_page_mapping_data[prod],
        prod,
        fake.random_int(1, 100000),
        duration,
        0 if status != "discontinued" and fake.random_int(0, 100) < 90 else 1,
        fake.random_int(1000)
      ))
      click_id += 1
    current_ts += timedelta(days=1)

clickstream_df = spark.createDataFrame(data=clickstream_data, schema=clickstream_schema)
clickstream_df.write.mode("overwrite").parquet(f"{adls_root}/clickstream")
# .saveAsTable(f"{data_area}.{current_user}_clickstream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read, transform and write from/to Azure Data Lake
# MAGIC - The code below shows how to load your data stored in Azure Data Lake Sandbox.
# MAGIC - Use the built-in data analysis capabilities to explore it.
# MAGIC - Transform datasets
# MAGIC - Write data back to the data lake.

# COMMAND ----------

## Initializing variables
current_user = spark.sql("select current_user()").collect()[0].asDict()["current_user()"].replace("@sap.com", "").replace(".", "_")

### The data lake is split into data area, each related to a use case or cluster of use cases
### The data area related to the sandbox is 'sbox'
data_area = "sbox"
storage_account = "coredatalakesandbox"
adls_root = f"abfss://{data_area}@{storage_account}.dfs.core.windows.net/store/{current_user}"

## Load data directly from the data lake. For this demo, we will load sales opportunities and customers clickstream events on SAP product related pages
opportunities_df = spark.read.parquet(f"{adls_root}/opportunities")
clickstream_df = spark.read.parquet(f"{adls_root}/clickstream")

# COMMAND ----------

display(opportunities_df)

# COMMAND ----------

display(clickstream_df)

# COMMAND ----------

## Join between opportunities and clickstream data events
df = opportunities_df.join(clickstream_df, (opportunities_df["logical_product"] == clickstream_df["product"]) \
                                            & (opportunities_df["customer"] == clickstream_df["customer"])) \
                     .select("opp_id", "contact_person", "dist_channel", "status", "created_at", "deal_size_keur", "logical_product", \
                              opportunities_df["customer"], "click_id", "timestamp", "client_ip", "server_ip", "user_agent", \
                              "product", "browsing_time_per_page", "num_errors")

## Write joined dataset in data lake
df.write.mode("overwrite").parquet(f"{adls_root}/clickstream_opps")

## To simplify the access and catalogingof your data, it is possible to expose your datasets into Hive tables that you can
## browse through the 'Data' tab on the left.
spark.sql(f"CREATE TABLE IF NOT EXISTS sbox.{current_user}_clickstream_opps USING PARQUET LOCATION '{adls_root}/clickstream_opps'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ML Experimentation
# MAGIC - Create a training dataset
# MAGIC - Experiment with different algorithms and tune hyperparameters
# MAGIC - Model selection
# MAGIC - Examine the learned feature importances output by the model as a sanity-check.

# COMMAND ----------

from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
import mlflow

data = spark.sql(f"""select opp_id, status, browsing_time_per_page, num_errors
                     from sbox.{current_user}_clickstream_opps
                     where status in ('won', 'discontinued')
                     limit 1000""").toPandas()

# COMMAND ----------

display(data)

# COMMAND ----------

data["status"] = data["status"].apply(lambda val: 1 if val == "won" else 0)
data_x = data.drop(["status", "opp_id"], axis=1)
x_train, x_test, y_train, y_test = train_test_split(data_x, data["status"])

# COMMAND ----------

from mlflow.models.signature import infer_signature

with mlflow.start_run(run_name='opportunities temperature') as run:
  model = SVC(kernel="linear", C=0.6)
  mlflow.log_param('algorithm', "SVM")
  mlflow.log_param('C', model.C)
  mlflow.log_param('kernel', model.kernel)

  model.fit(x_train, y_train)
  accuracy = model.score(x_test, y_test)
  mlflow.log_metric('accuracy', accuracy)

  # Log the model with a signature that defines the schema of the model's inputs and outputs. 
  # When the model is deployed, this signature will be used to validate inputs.
  signature = infer_signature(x_train, model.predict(x_train))
  mlflow.sklearn.log_model(model, artifact_path="model", signature=signature)

  run_id = run.info.run_id

# COMMAND ----------

import pandas as pd
from sklearn.inspection import permutation_importance

## Examine feature importance
perm_importance = permutation_importance(model, x_test, y_test)

df = pd.DataFrame()
df["importance_mean"] = perm_importance.importances_mean
df["features"] = x_train.columns
display(df)

# COMMAND ----------

# MAGIC %md ## Registering the model in the MLflow Model Registry
# MAGIC By registering this model in the Model Registry, you can easily reference the model from anywhere within Databricks.
# MAGIC
# MAGIC The following section shows how to do this programmatically, but you can also register a model using the UI by following the steps in Register a model in the Model Registry .

# COMMAND ----------

model_version = mlflow.register_model(f"runs:/{run_id}/model", f"{current_user}_opportunity_temperature")

# COMMAND ----------

# MAGIC %md ## Batch Inference
# MAGIC There are many scenarios where you might want to evaluate a model on a corpus of new data. For example, you may have a fresh batch of data, or may need to compare the performance of two models on the same corpus of data.
# MAGIC
# MAGIC The following code evaluates the model on the opportunities and the related clickstream activity which are still 'in progress', using Spark to run the computation in parallel.

# COMMAND ----------

import mlflow.pyfunc
%pip install flask
import flask
 
apply_model_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_version.name}/{model_version.version}")

# COMMAND ----------

data = spark.sql(f"""select opp_id, browsing_time_per_page, num_errors
                     from sbox.{current_user}_clickstream_opps
                     where status = 'in progress'
                     limit 1000""")

# COMMAND ----------

from pyspark.sql.functions import struct
 
# Apply the model to the new data
udf_inputs = struct(*(data.columns))

opp_id = data.select("opp_id")
data = data.withColumn("prediction", apply_model_udf(udf_inputs)).withColumn("opp_id", opp_id.opp_id)

# COMMAND ----------

display(data)

# COMMAND ----------


