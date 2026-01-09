AWC DealRadar

Version	0.2
Author	Halvor Lande
Date	09.01.2026

DealRadar is an AI-powered, continuously learning engine that identifies and prioritizes relevant investment opportunities for AWC. 
DealRadar is first developed for Norwegian opportunities. If it works well, it will be expanded to other jurisdictions over time.  
Scope
Unlisted Norwegian companies (AS and ASA) with >50 MNOK in EBIT
Excludes public sector companies (government-controlled/public administration).
Can be expanded to other jurisdictions and investment types over time
Functionality
•	Tracks financial performance and AWC investment status of all Norwegian companies (AS and ASA) with over 50 MNOK in EBIT
•	Tracks news on companies with all investment statuses other than “Dead” or “Not relevant for AWC”, and generates triggers if relevant
•	Prioritizes Prospect list based on ownership structure, news, and financial performance
Investment status
1.	Prospect: Seems like a potential investment opportunity
2.	Early: Initial analysis and/or discussions ongoing
3.	Investing: Due diligence and negotiations
4.	Invested: AWC currently owns shares in the company
5.	Keep warm: Ongoing dialogue/monitoring
6.	Parked: Not relevant right now
7.	Not relevant for AWC: Excluded from monitoring and ranking unless manually reactivated
8.	Dead: Permanently excluded (e.g., liquidated, acquired, or does not fit AWC mandate)
9.	Exited: AWC has sold its shares in the company
Prospect ranking
Our highest priority opportunities are high-quality businesses + high probability proprietary access + clear reason to act now + low competitive intensity. Accordingly, prospects are ranked according to the quality of the underlying business, adjusted for likelihood of a successful investment deal and deal competition:
PriorityScore = QualityScore * DealLikelihoodScore - CompetitionPenalty
QualityScore
DealRadar focuses primarily on finding compounders: high return on capital + growth + moat. Due to our small team size we prefer larger deals; minimum investment 200 MNOK, but ideal investment size 500 - 2000 MNOK.
QualityScore is composed of two components: BusinessQualityScore (BQS) and DeployabilityScore (DPS):
QualityScore = 70% BQS + 30% DPS
BusinessQualityScore (BQS)
This is the score that aim to capture “real world moat” in financial terms: High profitability and high return on capital over time:
BQS = 30% ROE + 20% EBIT + 20% Growth + 20% Cash Conversion + 10% Robustness, where:
•	ROE: Return on equity, measured as net income / average equity (3-year average, where available).
•	EBIT: Earnings before interest and tax, measured in NOK and as EBIT margin (3-year average).
•	Growth: Revenue CAGR and EBIT CAGR (3-year, where available), with penalty for margin deterioration.
•	Cash Conversion: Operating cash flow / EBITDA (or / EBIT if EBITDA not available), 3-year average.
•	Robustness: Balance sheet strength (equity ratio) and leverage (net debt / EBITDA or proxy).
DeployabilityScore (DPS)
DeployabilityScore captures AWC's preference for fewer, larger transactions. Given a small investment team and an annual deployment target of approximately NOK 5bn in 2026 (expected to increase over time), larger investments are preferred to reduce total transaction count, board load, and ongoing governance effort.
DPS = 60% ScaleScore + 40% TicketFitScore, where:
•	ScaleScore: Rewards companies with high EBIT/EBITDA and revenue that can support a large equity check without requiring majority ownership.
•	TicketFitScore: Rewards companies where an indicative minority investment size is in the preferred range (typically NOK 500-2,000m; minimum NOK 200m).
DealLikelihoodScore
DealLikelihoodScore estimates the probability of sourcing and completing an investment at attractive terms. It rewards situations with likely proprietary access and a clear reason to act now.
DealLikelihoodScore is composed of the following sub-scores (initial weights; to be tuned based on outcomes):
•	Ownership & succession (35%): fragmented ownership, family ownership with generational transition indicators, and absence of financial sponsor control.
•	Capital need with strong performance (20%): balance sheet/working capital indicators of growth or refinancing needs, while maintaining strong profitability.
•	Freshness & momentum (20%): recently published annual accounts and positive year-over-year improvement in key metrics.
•	Moat and management proxies (15%): stable high margins/returns over time, low volatility, and qualitative flags from AWC review.
•	Engagement fit (10%): ability to invest as a minority shareholder and align with founders/owners (can be partially manual).
CompetitionPenalty
CompetitionPenalty reduces priority where a transaction is likely to be auction-based or otherwise highly competitive (e.g., investment bank-led sale processes). This component is expected to be imperfect and will initially rely on a combination of data flags and analyst input.
Indicative penalty triggers:
•	Financial sponsor ownership (PE/VC) or recent sponsor entry
•	Publicly announced sale/strategic review, advisor mandates, or auction-related news
•	Frequent M&A activity suggesting continuous market testing
•	Recent fundraising rounds with strong competitive momentum (where relevant)
User roles and workflows
DealRadar is used by the AWC investment team to identify, prioritize, and manage proactive outreach to investment prospects. The system supports both an individual-user prototype and a future multi-user deployment.
User roles
•	Admin: Manages configuration, scoring parameters, data sources, and user access.
•	Investor/User: Reviews daily Top 10, deep-dives companies, and logs outreach activities and status changes.
•	Read-only: Optional role for leadership/support functions (view dashboards without editing).
Daily operating model
Every business day, DealRadar generates a prioritized shortlist of 10 companies for proactive outreach. The shortlist is created after ingesting new financial statements and relevant news, recalculating scores, and applying de-duplication rules to avoid repeatedly surfacing companies that have been contacted within the last 12 months (default outreach cooldown).
Key steps:
•	Ingest new or updated annual accounts since the last run.
•	Recompute QualityScore and DealLikelihoodScore for impacted companies.
•	Update investment status and outreach activity from user input.
•	Generate the Top 10 shortlist for the day, with a brief explanation ('why this company, why now').
•	Investment directors self-select opportunities by claiming them in the system (to avoid duplicate work).
•	Claiming is logged (claimedBy, claimedAt) and prevents others from claiming the same opportunity while it is in progress.
•	The investment director logs relevant analysis, decisions, internal discussion, and company communication (calls/emails/meetings) on the company profile.
•	The investment director updates status, next steps, and a follow-up date; the system enforces a 12-month outreach cooldown unless manually overridden.
Functional requirements
The requirements below describe the minimum viable product (MVP) for a local prototype, and the target capabilities for a production deployment. Each requirement is tagged with a priority: Must (MVP), Should (next iteration), or Could (future).
ID	Requirement	Priority	Acceptance criteria / notes
FR-01	The system shall maintain a master list of Norwegian companies (AS and ASA) with identifiers (orgnr), name, and basic metadata, excluding public sector companies.	Must	Company records can be created/updated and queried by orgnr. Public sector companies are excluded based on provider flags/rules, with manual override.
FR-02	The system shall store annual financial statements and derived metrics per company and year.	Must	At least revenue, EBIT, assets, and equity are stored when available.
FR-03	The system shall compute QualityScore for each company based on stored financials and parameters.	Must	Score is reproducible and the breakdown is visible to the user.
FR-04	The system shall compute DealLikelihoodScore and CompetitionPenalty and produce PriorityScore = QualityScore * DealLikelihoodScore - CompetitionPenalty.	Must	PriorityScore can be recalculated on demand and is stored with timestamp.
FR-05	The system shall generate a daily Top 10 shortlist with rank, key figures, and a short 'reason summary'.	Must	Top 10 can be viewed in UI and retrieved via API.
FR-11	The system should allow investment directors to claim (self-select) a prospect to avoid duplicate work, and display claim status to other users.	Should	Claiming is atomic and logged (claimedBy, claimedAt). Claimed prospects cannot be claimed by others until released or completed.
FR-12	The system should provide an activity log per company for analysis notes, decisions, and company communication (calls/emails/meetings), with timestamps and author.	Should	Activity log is visible on company profile and is auditable/exportable.
FR-06	The system shall support investment status management (Prospect, Early, Investing, Invested, Keep warm, Parked, Exited, Dead/Not relevant).	Must	Status changes are auditable and reflected in Top 10 selection rules.
FR-07	The system shall allow users to assign an internal owner and log outreach notes, next steps, and last contact date.	Must	Updates persist and are visible on company profile. System tracks last contact date and enforces a default 12-month outreach cooldown.
FR-08	The system should ingest financial statement updates from Proff Premium API, including detection of newly published annual accounts for a date or period, and maintain at least 3 years of history where available.	Should	Daily run updates only impacted companies; ingestion job logs successes/failures; supports delta feed / changes since last run.
FR-09	The system should ingest and classify news for companies that are not marked Dead/Not relevant, and generate triggers (MVP: Grok realtime API).	Should	News triggers are displayed and can influence ranking.
FR-10	The system could support Teams integration (tab + notifications) and multi-user access control via Entra ID groups.	Could	Users can access via SSO and see role-based permissions.
Data requirements and sources
DealRadar depends on structured company and financial data, as well as ownership and news signals. In production, company and financial data will be sourced from Proff Premium API (financials, ownership, contacts), and news signals will be sourced from Grok realtime API (MVP). Data sources must support bulk access and incremental updates.
Minimum data fields (MVP)
•	Company: orgnr, name, company type (AS/ASA), status (active/liquidated), NACE, municipality.
•	Financial statement (latest year): revenue, EBIT, net income (if available), assets, equity, total liabilities, filing/publish date.
•	Derived: EBIT margin, equity ratio, ROE proxy, growth proxies when history exists.
Commercial provider requirements
•	API or feed that supports 'changes since' or 'new annual accounts between date A and B' (delta feed).
•	Multi-year history for key figures (minimum: 3 years; preferred: 5+ years if available).
•	Ownership and role data (shareholders/UBO, board, CEO) sufficient to estimate fragmentation and succession signals.
•	Contact data (switchboard/website/phone/email) where legally and contractually permitted.
•	Clear licensing terms allowing internal storage and derived scoring.
Non-functional requirements
ID	Category	Requirement
NFR-01	Performance	Daily update job completes within a configurable window and does not block user access.
NFR-02	Reliability	Ingestion is idempotent and resilient to transient API errors (retry/backoff + logging).
NFR-03	Auditability	Key events are logged: data ingestion timestamps, score recalculation runs, status changes, and outreach edits.
NFR-04	Security	Production deployment uses SSO and least-privilege access to data stores; secrets are not stored in code.
NFR-05	Maintainability	Scoring parameters are configurable without code changes, and changes are versioned.
Architecture (target)
The prototype is developed locally (Python + React + SQL Server). If the project proceeds, the target production architecture is expected to use Azure SQL as the system of record, a hosted web application for the UI and API, and scheduled ingestion jobs for daily updates. Authentication and authorization should be integrated with AWC's Microsoft Entra ID and group-based access control.
Decisions
•	Commercial data provider: Proff Premium API.
•	News provider (MVP): Grok realtime API.
•	Financial history window for scoring: 3 years (where available).
•	Outreach repetition / cooldown: 12 months (default).
•	Exclude public sector companies from the investable universe (unless manually overridden).

