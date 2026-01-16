const { useEffect, useMemo, useState } = React;

const API_BASE = window.API_BASE || "http://localhost:8000";

const demoCompanies = [
  {
    orgnr: "999888777",
    name: "Northwind Logistics",
    total_score: 82.4,
    compounder_score: 78.6,
    deployability: 0.72,
    urgency: 18,
  },
  {
    orgnr: "555444333",
    name: "Aurora Software",
    total_score: 76.1,
    compounder_score: 69.2,
    deployability: 0.48,
    urgency: 32,
  },
  {
    orgnr: "222111000",
    name: "Fjord Manufacturing",
    total_score: 68.3,
    compounder_score: 62.9,
    deployability: 0.63,
    urgency: 12,
  },
];

const demoDetails = {
  "999888777": {
    orgnr: "999888777",
    name: "Northwind Logistics",
    description:
      "Northwind Logistics runs a scalable transport network across the Nordics, specializing in cold-chain distribution.",
    deployability: 0.72,
    deployability_explanation:
      "Large regional footprint and clear add-on acquisition runway make this an attractive deployment target.",
    urgency: 18,
    urgency_explanation:
      "Ownership is stable, but consolidation pressure is building from European peers.",
    roic: 0.21,
    roic_score: 30,
    revenue_cagr: 0.14,
    revenue_cagr_score: 15,
    margin_change: 0.025,
    margin_change_score: 20,
    cash_conversion: 0.92,
    nwc_sales: 0.12,
    nwc_sales_score: 10,
    goodwill_ratio: 0.25,
    goodwill_ratio_score: 10,
  },
  "555444333": {
    orgnr: "555444333",
    name: "Aurora Software",
    description:
      "Aurora Software delivers vertical SaaS for regulated utilities with high recurring revenue retention.",
    deployability: 0.48,
    deployability_explanation:
      "Smaller ticket size and founder control increase execution overhead for larger capital deployment.",
    urgency: 32,
    urgency_explanation:
      "Key contracts are up for renewal within 12 months, offering a timing opportunity.",
    roic: 0.16,
    roic_score: 24,
    revenue_cagr: 0.08,
    revenue_cagr_score: 8,
    margin_change: 0.01,
    margin_change_score: 12,
    cash_conversion: 1.05,
    nwc_sales: 0.05,
    nwc_sales_score: 12,
    goodwill_ratio: 0.42,
    goodwill_ratio_score: 6,
  },
  "222111000": {
    orgnr: "222111000",
    name: "Fjord Manufacturing",
    description:
      "Fjord Manufacturing builds engineered components for offshore energy infrastructure.",
    deployability: 0.63,
    deployability_explanation:
      "Mid-market scale with consistent cash flows, but capex cycle may slow deployment pace.",
    urgency: 12,
    urgency_explanation:
      "Long-term supply agreements reduce near-term urgency.",
    roic: 0.09,
    roic_score: 8,
    revenue_cagr: -0.04,
    revenue_cagr_score: -5,
    margin_change: -0.015,
    margin_change_score: -8,
    cash_conversion: 0.55,
    nwc_sales: 0.28,
    nwc_sales_score: -6,
    goodwill_ratio: 0.65,
    goodwill_ratio_score: -4,
  },
};

const formatNumber = (value, decimals = 1) =>
  value === null || value === undefined ? "—" : Number(value).toFixed(decimals);

const formatPercent = (value, decimals = 0) =>
  value === null || value === undefined
    ? "—"
    : `${(Number(value) * 100).toFixed(decimals)}%`;

const formatPercentPoints = (value, decimals = 1) => {
  if (value === null || value === undefined) {
    return "—";
  }
  const formatted = (Number(value) * 100).toFixed(decimals);
  const sign = Number(value) > 0 ? "+" : "";
  return `${sign}${formatted} pp`;
};

const formatScore = (value) => {
  if (value === null || value === undefined) {
    return "—";
  }
  const sign = Number(value) > 0 ? "+" : "";
  return `${sign}${Number(value).toFixed(0)} pts`;
};

const badgeClassForValue = ({
  value,
  positiveThreshold,
  negativeThreshold,
  invert = false,
}) => {
  if (value === null || value === undefined) {
    return "badge-neutral";
  }
  const numeric = Number(value);
  if (invert) {
    if (numeric < positiveThreshold) {
      return "badge-positive";
    }
    if (numeric > negativeThreshold) {
      return "badge-negative";
    }
  } else {
    if (numeric > positiveThreshold) {
      return "badge-positive";
    }
    if (numeric < negativeThreshold) {
      return "badge-negative";
    }
  }
  return "badge-neutral";
};

const DashboardTile = ({ title, value, score, badgeClass }) => (
  <div className="metric-tile">
    <div className="metric-header">
      <span className="metric-title">{title}</span>
      <span className={`metric-score ${badgeClass}`}>{formatScore(score)}</span>
    </div>
    <div className="metric-value">{value}</div>
  </div>
);

const App = () => {
  const [companies, setCompanies] = useState([]);
  const [selectedOrgnr, setSelectedOrgnr] = useState(null);
  const [detail, setDetail] = useState(null);
  const [status, setStatus] = useState("Loading companies...");
  const [sortConfig, setSortConfig] = useState({
    key: "name",
    direction: "asc",
  });
  const [isDemo, setIsDemo] = useState(false);

  useEffect(() => {
    const loadCompanies = async () => {
      try {
        const response = await fetch(`${API_BASE}/companies`);
        if (!response.ok) {
          throw new Error("Failed to fetch companies");
        }
        const data = await response.json();
        setCompanies(data);
        if (data.length > 0) {
          setSelectedOrgnr(data[0].orgnr);
        }
        setStatus("Live data");
      } catch (error) {
        setIsDemo(true);
        setCompanies(demoCompanies);
        setSelectedOrgnr(demoCompanies[0].orgnr);
        setStatus("Using demo data (API unavailable)");
      }
    };

    loadCompanies();
  }, []);

  useEffect(() => {
    if (!selectedOrgnr) {
      return;
    }

    const loadDetail = async () => {
      if (isDemo) {
        setDetail(demoDetails[selectedOrgnr]);
        return;
      }
      try {
        const response = await fetch(`${API_BASE}/companies/${selectedOrgnr}`);
        if (!response.ok) {
          throw new Error("Failed to fetch company detail");
        }
        const data = await response.json();
        setDetail(data);
      } catch (error) {
        setDetail(null);
      }
    };

    loadDetail();
  }, [selectedOrgnr, isDemo]);

  const sortedCompanies = useMemo(() => {
    const sorted = [...companies];
    const { key, direction } = sortConfig;
    sorted.sort((a, b) => {
      const aValue = a[key];
      const bValue = b[key];

      if (aValue === null || aValue === undefined) return 1;
      if (bValue === null || bValue === undefined) return -1;

      if (typeof aValue === "string") {
        const comparison = aValue.localeCompare(bValue);
        return direction === "asc" ? comparison : -comparison;
      }

      const comparison = Number(aValue) - Number(bValue);
      return direction === "asc" ? comparison : -comparison;
    });
    return sorted;
  }, [companies, sortConfig]);

  const handleSort = (key) => {
    setSortConfig((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === "asc" ? "desc" : "asc",
        };
      }
      return { key, direction: "asc" };
    });
  };

  const detailReady = detail && detail.name;

  const dashboardTiles = detailReady
    ? [
        {
          title: "ROIC (Avg)",
          value: formatPercent(detail.roic, 0),
          score: detail.roic_score,
          badgeClass: badgeClassForValue({
            value: detail.roic,
            positiveThreshold: 0.15,
            negativeThreshold: 0.1,
          }),
        },
        {
          title: "Growth (CAGR)",
          value: formatPercent(detail.revenue_cagr, 0),
          score: detail.revenue_cagr_score,
          badgeClass: badgeClassForValue({
            value: detail.revenue_cagr,
            positiveThreshold: 0.1,
            negativeThreshold: 0,
          }),
        },
        {
          title: "Moat Trend",
          value: formatPercentPoints(detail.margin_change, 1),
          score: detail.margin_change_score,
          badgeClass: badgeClassForValue({
            value: detail.margin_change,
            positiveThreshold: 0,
            negativeThreshold: 0,
          }),
        },
        {
          title: "Cash Conv.",
          value: formatPercent(detail.cash_conversion, 0),
          score: null,
          badgeClass: badgeClassForValue({
            value: detail.cash_conversion,
            positiveThreshold: 0.9,
            negativeThreshold: 0.6,
          }),
        },
        {
          title: "NWC / Sales",
          value: formatPercent(detail.nwc_sales, 0),
          score: detail.nwc_sales_score,
          badgeClass: badgeClassForValue({
            value: detail.nwc_sales,
            positiveThreshold: 0.1,
            negativeThreshold: 0.25,
            invert: true,
          }),
        },
        {
          title: "Goodwill Ratio",
          value: formatPercent(detail.goodwill_ratio, 0),
          score: detail.goodwill_ratio_score,
          badgeClass: badgeClassForValue({
            value: detail.goodwill_ratio,
            positiveThreshold: 0.3,
            negativeThreshold: 0.6,
            invert: true,
          }),
        },
      ]
    : [];

  return (
    <div className="app-shell">
      <section className="pane pane-left">
        <div className="header">
          <h1>Company Scores</h1>
          <span className="status">{status}</span>
        </div>
        <div className="table-wrapper">
          <table className="table">
            <thead>
              <tr>
                <th onClick={() => handleSort("name")}>Company</th>
                <th onClick={() => handleSort("total_score")}>Overall</th>
                <th onClick={() => handleSort("compounder_score")}>Quality</th>
                <th onClick={() => handleSort("deployability")}>Deployability</th>
                <th onClick={() => handleSort("urgency")}>Urgency</th>
              </tr>
            </thead>
            <tbody>
              {sortedCompanies.map((company) => (
                <tr
                  key={company.orgnr}
                  className={
                    company.orgnr === selectedOrgnr ? "active" : undefined
                  }
                  onClick={() => setSelectedOrgnr(company.orgnr)}
                >
                  <td className="company-name">{company.name}</td>
                  <td>{formatNumber(company.total_score, 1)}</td>
                  <td>{formatNumber(company.compounder_score, 1)}</td>
                  <td>{formatPercent(company.deployability, 0)}</td>
                  <td>{formatNumber(company.urgency, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pane pane-right">
        {!detailReady ? (
          <div className="placeholder">
            Select a company to view detailed diagnostics.
          </div>
        ) : (
          <>
            <div className="detail-section">
              <div className="section-title">
                <span>Quality</span>
                <span className="status">{detail.name}</span>
              </div>
              <div className="detail-grid">
                {dashboardTiles.map((tile) => (
                  <DashboardTile key={tile.title} {...tile} />
                ))}
              </div>
            </div>

            <div className="detail-section">
              <div className="section-title">
                <span>
                  Deployability {formatPercent(detail.deployability, 0)}
                </span>
              </div>
              <div className="detail-block">
                {detail.deployability_explanation || "No deployability notes."}
              </div>
            </div>

            <div className="detail-section">
              <div className="section-title">
                <span>Urgency {formatNumber(detail.urgency, 0)}</span>
              </div>
              <div className="detail-block">
                {detail.urgency_explanation || "No urgency notes."}
              </div>
            </div>

            <div className="detail-section">
              <div className="section-title">
                <span>About the company</span>
              </div>
              <div className="detail-block">
                {detail.description || "No description available."}
              </div>
            </div>

            <div className="footer-actions">
              <button>Not relevant for AWC</button>
              <button className="secondary">
                Create company folder in Deals
              </button>
              <button className="primary">Revisit later</button>
            </div>
          </>
        )}
      </section>
    </div>
  );
};

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
